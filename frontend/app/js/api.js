/* Thin fetch wrapper: JSON in/out, normalised errors, timeouts. */
export class ApiError extends Error {
    constructor(message, status, detail) { super(message); this.status = status; this.detail = detail; }
}

function detailToText(detail) {
    if (detail == null) return '';
    if (typeof detail === 'string') return detail;
    if (Array.isArray(detail)) return detail.map(d => (d.loc ? d.loc.join('.') + ': ' : '') + (d.msg || JSON.stringify(d))).join('; ');
    return JSON.stringify(detail);
}

async function request(method, url, body, { timeout = 60000, raw = false } = {}) {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), timeout);
    try {
        const res = await fetch(url, {
            method,
            headers: body !== undefined ? { 'Content-Type': 'application/json' } : {},
            body: body !== undefined ? JSON.stringify(body) : undefined,
            signal: ctrl.signal,
        });
        const text = await res.text();
        let data = text;
        if (!raw) { try { data = text ? JSON.parse(text) : null; } catch { data = text; } }
        if (!res.ok) {
            const detail = data && typeof data === 'object' ? data.detail : data;
            throw new ApiError(detailToText(detail) || `${res.status} ${res.statusText}`, res.status, detail);
        }
        return data;
    } catch (e) {
        if (e.name === 'AbortError') throw new ApiError(`request timed out (${url})`, 0);
        if (e instanceof ApiError) throw e;
        throw new ApiError(e.message || 'network error', 0);
    } finally { clearTimeout(timer); }
}

const qs = (params) => {
    const p = Object.entries(params || {}).filter(([, v]) => v !== undefined && v !== null && v !== '');
    return p.length ? '?' + p.map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`).join('&') : '';
};

export const api = {
    get: (url, params, opts) => request('GET', url + qs(params), undefined, opts),
    post: (url, body, opts) => request('POST', url, body, opts),
    put: (url, body, opts) => request('PUT', url, body, opts),
    del: (url, opts) => request('DELETE', url, undefined, opts),

    // convenience groups
    health: () => request('GET', '/health'),
    settings: { info: () => request('GET', '/api/settings/info'), export: (s) => request('GET', '/api/settings/export' + qs({ include_secrets: s })), import: (data, overwrite) => request('POST', '/api/settings/import', { data, overwrite }) },
    connections: {
        list: () => request('GET', '/api/admin/connection/list'),
        get: (id) => request('GET', `/api/admin/connection/${encodeURIComponent(id)}`),
        save: (c) => request('POST', '/api/admin/connection/save', c),
        update: (c) => request('PUT', '/api/admin/connection/update', c),
        remove: (id) => request('DELETE', `/api/admin/connection/${encodeURIComponent(id)}`),
        test: (c) => request('POST', '/api/admin/connect/test', c, { timeout: 30000 }),
    },
    connectors: {
        engines: () => request('GET', '/api/connectors/engines'),
        test: (b) => request('POST', '/api/connectors/test', b, { timeout: 30000 }),
        prerequisites: (b) => request('POST', '/api/connectors/prerequisites', b, { timeout: 120000 }),
        captureStatus: (b) => request('POST', '/api/connectors/capture/status', b),
        enableDb: (b) => request('POST', '/api/connectors/capture/enable-database', b, { timeout: 120000 }),
        enableTables: (b) => request('POST', '/api/connectors/capture/enable-tables', b, { timeout: 120000 }),
        disableTables: (b) => request('POST', '/api/connectors/capture/disable-tables', b, { timeout: 120000 }),
        tables: (b) => request('POST', '/api/connectors/tables', b, { timeout: 120000 }),
        columns: (b) => request('POST', '/api/connectors/columns', b),
        position: (b) => request('POST', '/api/connectors/position', b),
        brokerScript: (p) => request('GET', '/api/connectors/mssql/service-broker-script' + qs(p)),
    },
    mappings: {
        list: () => request('GET', '/api/admin/mapping/list'),
        get: (id) => request('GET', `/api/admin/mapping/${encodeURIComponent(id)}`),
        create: (m) => request('POST', '/api/admin/mapping/create', m),
        update: (m) => request('PUT', '/api/admin/mapping/update', m),
        remove: (id) => request('DELETE', `/api/admin/mapping/${encodeURIComponent(id)}`),
        test: (id) => request('POST', `/api/admin/mapping/${encodeURIComponent(id)}/test`, undefined, { timeout: 120000 }),
        history: (id) => request('GET', `/api/admin/mapping/${encodeURIComponent(id)}/history`),
        verify: (b) => request('POST', '/api/monitoring/verify', b, { timeout: 300000 }),
        execute: (id) => request('POST', `/api/operations/mapping/${encodeURIComponent(id)}/execute`, undefined, { timeout: 600000 }),
    },
    worksets: {
        list: () => request('GET', '/api/admin/workset/list'),
        create: (w) => request('POST', '/api/admin/workset/create', w),
        update: (w) => request('PUT', '/api/admin/workset/update', w),
        activate: (id) => request('PUT', `/api/admin/workset/activate/${encodeURIComponent(id)}`),
        remove: (id) => request('DELETE', `/api/admin/workset/${encodeURIComponent(id)}`),
    },
    operations: {
        start: () => request('POST', '/api/operations/start', undefined, { timeout: 120000 }),
        stop: () => request('POST', '/api/operations/stop', undefined, { timeout: 60000 }),
        status: () => request('GET', '/api/operations/status'),
    },
    pipelines: {
        list: () => request('GET', '/api/pipelines'),
        define: (d) => request('POST', '/api/pipelines', d),
        remove: (id) => request('DELETE', `/api/pipelines/${encodeURIComponent(id)}`),
        action: (id, a) => request('POST', `/api/pipelines/${encodeURIComponent(id)}/${a}`, undefined, { timeout: 60000 }),
        status: (id) => request('GET', `/api/pipelines/${encodeURIComponent(id)}/status`),
        health: (id) => request('GET', `/api/pipelines/${encodeURIComponent(id)}/health`),
        resetPositions: (id, table) => request('POST', `/api/pipelines/${encodeURIComponent(id)}/positions/reset` + qs({ table })),
        bus: () => request('GET', '/api/pipelines/bus/stats'),
    },
    snapshots: {
        list: (state) => request('GET', '/api/snapshots' + qs({ state })),
        create: (b) => request('POST', '/api/snapshots', b, { timeout: 120000 }),
        get: (id) => request('GET', `/api/snapshots/${encodeURIComponent(id)}`),
        action: (id, a) => request('POST', `/api/snapshots/${encodeURIComponent(id)}/${a}`),
        remove: (id) => request('DELETE', `/api/snapshots/${encodeURIComponent(id)}`),
    },
    audit: {
        events: (p) => request('GET', '/api/audit/events' + qs(p)),
        stats: (hours) => request('GET', '/api/audit/stats' + qs({ hours })),
        failures: (p) => request('GET', '/api/audit/failures' + qs(p)),
        failure: (id) => request('GET', `/api/audit/failures/${id}`),
        patterns: (p) => request('GET', '/api/audit/patterns' + qs(p)),
        resolve: (b) => request('POST', '/api/audit/failures/resolve', b),
        replay: (b) => request('POST', '/api/audit/failures/replay', b, { timeout: 300000 }),
        applyPattern: (b) => request('POST', '/api/audit/patterns/apply', b),
        purge: (p) => request('POST', '/api/audit/purge' + qs(p)),
    },
    alerts: {
        snapshot: () => request('GET', '/api/alerts'),
        evaluate: () => request('POST', '/api/alerts/evaluate'),
        context: () => request('GET', '/api/alerts/context'),
        ruleSets: () => request('GET', '/api/alerts/rule-sets'),
        setRuleSets: (enabled) => request('PUT', '/api/alerts/rule-sets', { enabled }),
        rules: () => request('GET', '/api/alerts/rules'),
        saveRule: (r) => request('PUT', '/api/alerts/rules', r),
        deleteRule: (id) => request('DELETE', `/api/alerts/rules/${encodeURIComponent(id)}`),
        validate: (condition) => request('POST', '/api/alerts/rules/validate', { condition }),
        channels: (b) => request('PUT', '/api/alerts/channels', b),
        test: () => request('POST', '/api/alerts/test'),
    },
    metrics: {
        json: () => request('GET', '/api/metrics'),
        history: (p) => request('GET', '/api/metrics/history' + qs(p)),
    },
};

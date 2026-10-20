/* Global reactive state: health, alerts, reference data, toasts, confirm dialog, live WebSocket feed. */
import { api } from './api.js';

const { reactive } = Vue;

export const store = reactive({
    loading: false,
    health: {},
    alerts: { active: [], recent_resolved: [], rules: [], channels: [] },
    connections: [],
    mappings: [],
    pipelines: [],
    snapshots: [],
    auditStats: null,
    toasts: [],
    confirm: { open: false, title: '', message: '', okLabel: 'OK', danger: false, _resolve: null },
    ws: { connected: false, events: [], socket: null },
    lastRefresh: null,
    _toastId: 0,

    // ------------------------------------------------------------ toasts
    toast(message, type = 'info', ttl = 5000) {
        const id = ++this._toastId;
        this.toasts.push({ id, message: String(message), type });
        if (ttl) setTimeout(() => this.dismissToast(id), ttl);
    },
    dismissToast(id) { this.toasts = this.toasts.filter(t => t.id !== id); },
    success(m) { this.toast(m, 'success'); },
    error(m) { this.toast(m, 'error', 8000); },
    warn(m) { this.toast(m, 'warning', 7000); },

    // ------------------------------------------------------------ confirm
    ask(message, { title = 'Please confirm', okLabel = 'OK', danger = false } = {}) {
        return new Promise(resolve => {
            this.confirm = { open: true, title, message, okLabel, danger, _resolve: resolve };
        });
    },
    resolveConfirm(value) {
        const r = this.confirm._resolve;
        this.confirm = { open: false, title: '', message: '', okLabel: 'OK', danger: false, _resolve: null };
        if (r) r(value);
    },

    // ------------------------------------------------------------ helper: run with error toast
    async run(fn, { successMessage = null, rethrow = false } = {}) {
        this.loading = true;
        try {
            const r = await fn();
            if (successMessage) this.success(successMessage);
            return r;
        } catch (e) {
            this.error(e.message || String(e));
            if (rethrow) throw e;
            return undefined;
        } finally { this.loading = false; }
    },

    // ------------------------------------------------------------ reference data
    async loadReference() {
        const [connections, mappings] = await Promise.all([api.connections.list(), api.mappings.list()]);
        this.connections = connections;
        this.mappings = mappings;
    },
    async loadRuntime() {
        const [health, alerts, pipelines, snapshots, auditStats] = await Promise.all([
            api.health(), api.alerts.snapshot(), api.pipelines.list(), api.snapshots.list(), api.audit.stats(24),
        ]);
        this.health = health; this.alerts = alerts; this.pipelines = pipelines; this.snapshots = snapshots; this.auditStats = auditStats;
        this.lastRefresh = new Date();
    },
    async refreshAll() {
        try { await Promise.all([this.loadReference(), this.loadRuntime()]); }
        catch (e) { this.error('refresh failed: ' + e.message); }
    },
    connectionById(id) { return this.connections.find(c => c.id === id); },
    mappingById(id) { return this.mappings.find(m => m.id === id); },
    runningPipelines() { return this.pipelines.filter(p => p.runtime && ['running', 'paused'].includes(p.runtime.state)).map(p => p.runtime); },

    // ------------------------------------------------------------ polling
    startPolling(intervalMs = 5000) {
        const tick = async () => { if (!document.hidden) { try { await this.loadRuntime(); } catch { /* offline */ } } };
        setInterval(tick, intervalMs);
    },

    // ------------------------------------------------------------ websocket
    connectWs() {
        try {
            const proto = location.protocol === 'https:' ? 'wss' : 'ws';
            const ws = new WebSocket(`${proto}://${location.host}/api/monitoring/events`);
            this.ws.socket = ws;
            ws.onopen = () => { this.ws.connected = true; };
            ws.onclose = () => { this.ws.connected = false; setTimeout(() => this.connectWs(), 4000); };
            ws.onerror = () => ws.close();
            ws.onmessage = (ev) => {
                let msg; try { msg = JSON.parse(ev.data); } catch { return; }
                if (msg.type === 'alert') {
                    const resolved = msg.status === 'resolved';
                    this.toast(`${resolved ? 'RESOLVED' : msg.severity.toUpperCase()}: ${msg.name} – ${msg.message}`, resolved ? 'success' : (msg.severity === 'critical' || msg.severity === 'error' ? 'error' : 'warning'), 10000);
                    api.alerts.snapshot().then(a => { this.alerts = a; }).catch(() => {});
                } else if (msg.type === 'cdc_event') {
                    this.ws.events.unshift(msg);
                    if (this.ws.events.length > 300) this.ws.events.length = 300;
                }
            };
            setInterval(() => { if (ws.readyState === 1) ws.send('ping'); }, 25000);
        } catch { /* no websocket support */ }
    },
});

// ------------------------------------------------------------------ formatting helpers (shared by pages)
export const fmt = {
    num(n, digits = 0) { if (n === null || n === undefined || n === '') return '–'; if (typeof n !== 'number') return String(n); return n.toLocaleString(undefined, { maximumFractionDigits: digits }); },
    pct(x) { return x === null || x === undefined ? '–' : `${Math.round(x * 100)}%`; },
    secs(s) { if (s === null || s === undefined) return '–'; if (s < 60) return `${s.toFixed(s < 10 ? 1 : 0)} s`; if (s < 3600) return `${Math.floor(s / 60)} m ${Math.round(s % 60)} s`; return `${(s / 3600).toFixed(1)} h`; },
    bytes(b) { if (!b && b !== 0) return '–'; const u = ['B', 'KB', 'MB', 'GB', 'TB']; let i = 0; while (b >= 1024 && i < u.length - 1) { b /= 1024; i++; } return `${b.toFixed(i ? 1 : 0)} ${u[i]}`; },
    ts(iso) { if (!iso) return '–'; const d = new Date(iso); return isNaN(d) ? String(iso) : d.toLocaleString(); },
    ago(iso) { if (!iso) return '–'; const s = (Date.now() - new Date(iso).getTime()) / 1000; if (s < 60) return `${Math.round(s)} s ago`; if (s < 3600) return `${Math.round(s / 60)} min ago`; if (s < 86400) return `${(s / 3600).toFixed(1)} h ago`; return `${(s / 86400).toFixed(1)} d ago`; },
    json(v) { try { return JSON.stringify(v, null, 2); } catch { return String(v); } },
    pos(p) { if (!p) return '–'; const v = p.value; return typeof v === 'object' ? JSON.stringify(v) : String(v); },
};

export function download(filename, content, type = 'application/json') {
    const blob = new Blob([typeof content === 'string' ? content : JSON.stringify(content, null, 2)], { type });
    const a = document.createElement('a'); a.href = URL.createObjectURL(blob); a.download = filename; a.click();
    setTimeout(() => URL.revokeObjectURL(a.href), 1000);
}

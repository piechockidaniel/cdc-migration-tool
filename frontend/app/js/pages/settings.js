import { api } from '../api.js';
import { store, fmt, download } from '../store.js';

export const Settings = {
    data() { return { store, fmt, info: null, includeSecrets: false, importText: '', overwrite: false, purge: { audit_days: 30, failures_days: 90 }, bus: null }; },
    async created() { await this.load(); },
    methods: {
        async load() { try { [this.info, this.bus] = await Promise.all([api.settings.info(), api.pipelines.bus()]); } catch (e) { store.error(e.message); } },
        async exportConfig() { const data = await store.run(() => api.settings.export(this.includeSecrets)); if (data) download(`cdcsync-config-${new Date().toISOString().slice(0, 10)}.json`, data); },
        onFile(ev) { const f = ev.target.files[0]; if (!f) return; const r = new FileReader(); r.onload = () => { this.importText = r.result; }; r.readAsText(f); },
        async importConfig() {
            let data; try { data = JSON.parse(this.importText); } catch { store.error('invalid JSON'); return; }
            if (!await store.ask(`Import ${Object.keys(data).filter(k => typeof data[k] === 'object').map(k => k + ' (' + Object.keys(data[k] || {}).length + ')').join(', ')}${this.overwrite ? ' – existing documents will be OVERWRITTEN' : ''}?`, { okLabel: 'Import', danger: this.overwrite })) return;
            const r = await store.run(() => api.settings.import(data, this.overwrite)); if (r) { store.success(`imported ${r.imported}, skipped ${r.skipped}`); this.importText = ''; await store.refreshAll(); }
        },
        async doPurge() { if (!await store.ask(`Delete audit records older than ${this.purge.audit_days} days and resolved failures older than ${this.purge.failures_days} days?`, { danger: true, okLabel: 'Purge' })) return; const r = await store.run(() => api.audit.purge(this.purge)); if (r) store.success(`deleted ${r.audit_deleted} audit rows, ${r.failures_deleted} failures`); },
    },
    template: `<div>
        <help-hint id="settings" title="Instance settings">
            Where things live: configuration documents (connections, mappings, pipelines, rules) are in the <strong>config store</strong> - SQLite by default with optimistic locking, so several operators and API clients can edit concurrently without clobbering each other; the <strong>audit store</strong> holds the trail, failures and metrics history.
            Both paths and most knobs are environment variables (<code>CDCSYNC_*</code>, see the configuration reference). Retention purges are manual and logged.
        </help-hint>
        <div class="row g-3">
        <div class="col-lg-6">
            <div class="card mb-3" data-tour="settings-instance"><div class="card-header"><span><i class="bi bi-info-circle me-1"></i> Instance</span></div><div class="card-body">
                <dl class="kv" v-if="info">
                    <dt>version</dt><dd>{{ info.version }}</dd><dt>python</dt><dd>{{ info.python }} · {{ info.platform }}</dd>
                    <dt>config store</dt><dd><span class="badge bg-dark">{{ info.config_backend }}</span> <span class="mono small">{{ info.config_dir }}</span></dd>
                    <dt>audit store</dt><dd><span class="mono small">{{ info.history_dir }}/audit.sqlite3</span> · level {{ info.audit_level }}</dd>
                    <dt>documents</dt><dd class="small">{{ Object.entries(info.collections).map(([k, v]) => k + ': ' + v).join(' · ') }}</dd>
                    <dt>kafka mirror</dt><dd>{{ info.kafka_mirror ? 'enabled' : 'off (CDCSYNC_KAFKA_BOOTSTRAP)' }}</dd>
                    <dt>alert interval</dt><dd>{{ info.alert_interval_s }} s</dd><dt>parallel snapshots</dt><dd>{{ info.max_parallel_snapshots }}</dd>
                </dl>
                <h6 class="muted small text-uppercase mt-3">Event bus</h6>
                <div v-if="bus" class="small">published {{ fmt.num(bus.published) }} · transports {{ bus.transports.join(', ') || 'none' }}
                    <div v-for="s in bus.subscriptions" :key="s.name" class="mono">{{ s.name }}: depth {{ s.depth }} · delivered {{ fmt.num(s.delivered) }} · dropped {{ s.dropped }}</div></div>
            </div></div>
            <div class="card"><div class="card-header"><span><i class="bi bi-trash3 me-1"></i> Retention</span></div><div class="card-body">
                <div class="row g-2 align-items-end">
                    <div class="col-4"><label class="form-label">Audit days</label><input v-model.number="purge.audit_days" type="number" class="form-control form-control-sm"></div>
                    <div class="col-4"><label class="form-label">Resolved failures days</label><input v-model.number="purge.failures_days" type="number" class="form-control form-control-sm"></div>
                    <div class="col-4"><button class="btn btn-outline-danger btn-sm w-100" @click="doPurge()">Purge now</button></div>
                </div>
            </div></div>
        </div>
        <div class="col-lg-6">
            <div class="card mb-3"><div class="card-header"><span><i class="bi bi-download me-1"></i> Export configuration</span></div><div class="card-body">
                <p class="small muted">Connections, mappings, working sets, positions, pipeline definitions and custom rules as one JSON document – reviewable and versionable in git.</p>
                <div class="form-check mb-2"><input v-model="includeSecrets" class="form-check-input" type="checkbox" id="inc-sec"><label class="form-check-label" for="inc-sec">include plain-text secrets (not recommended – prefer <code>\${env:…}</code> references)</label></div>
                <button class="btn btn-primary btn-sm" @click="exportConfig()"><i class="bi bi-download"></i> Download JSON</button>
            </div></div>
            <div class="card"><div class="card-header"><span><i class="bi bi-upload me-1"></i> Import configuration</span></div><div class="card-body">
                <input type="file" accept="application/json" class="form-control form-control-sm mb-2" @change="onFile">
                <textarea v-model="importText" class="form-control mono mb-2" rows="6" placeholder="…or paste the JSON here"></textarea>
                <div class="d-flex align-items-center gap-3"><div class="form-check"><input v-model="overwrite" class="form-check-input" type="checkbox" id="ovw"><label class="form-check-label" for="ovw">overwrite existing documents</label></div>
                    <button class="btn btn-primary btn-sm" :disabled="!importText" @click="importConfig()"><i class="bi bi-upload"></i> Import</button></div>
            </div></div>
        </div>
    </div></div>`,
};

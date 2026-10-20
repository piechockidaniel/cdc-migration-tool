import { api } from '../api.js';
import { store, fmt } from '../store.js';

export const Dashboard = {
    data() { return { store, fmt, history: {}, patterns: [], audit: [], timer: null, range: 60 }; },
    computed: {
        pipelinesRunning() { return store.pipelines.filter(p => p.runtime && p.runtime.state === 'running').length; },
        pipelinesTotal() { return store.pipelines.length; },
        stats() { return store.auditStats || {}; },
        unresolved() { return this.stats.failures_total != null ? this.stats.failures_total - this.stats.failures_resolved : 0; },
        throughput() {
            return [{ label: 'applied/s', data: this.history['rate.applied'] || [], color: '#10b981' }, { label: 'failed/s', data: this.history['rate.failed'] || [], color: '#ef4444' }];
        },
        lagSeries() { return Object.entries(this.history).filter(([k]) => k.startsWith('lag.')).map(([k, v]) => ({ label: k.slice(4), data: v })); },
        queueSeries() { return Object.entries(this.history).filter(([k]) => k.startsWith('queue.')).map(([k, v]) => ({ label: k.slice(6), data: v })); },
        runtimePipelines() { return store.pipelines.filter(p => p.runtime).map(p => p.runtime); },
        activeSnapshots() { return store.snapshots.filter(j => ['running', 'paused', 'pending'].includes(j.state)); },
        worstLag() { const l = this.runtimePipelines.map(p => p.lag_seconds).filter(v => v != null); return l.length ? Math.max(...l) : null; },
    },
    async created() { await this.load(); this.timer = setInterval(() => this.loadHistory(), 10000); },
    unmounted() { clearInterval(this.timer); },
    methods: {
        async load() {
            await Promise.all([this.loadHistory(), this.loadSide()]);
        },
        async loadHistory() { try { this.history = (await api.metrics.history({ minutes: this.range })).series; } catch { /* ignore */ } },
        async loadSide() {
            try {
                const [p, a] = await Promise.all([api.audit.patterns({ hours: 24 }), api.audit.events({ hours: 24, limit: 12 })]);
                this.patterns = p.patterns.slice(0, 5); this.audit = a;
            } catch (e) { store.error(e.message); }
        },
    },
    template: `<div>
        <help-hint id="dashboard" title="How to read this page">
            The flow is <strong>Connections → Mappings → Pipelines → Snapshots</strong>; this page is the 24-hour summary of what they do. KPIs come from the audit store, charts from the metrics history.
            <em>Worst lag</em> is the oldest unapplied change across running pipelines, the <em>dead-letter queue</em> holds rows that failed to apply (they never block the stream), and <em>patterns</em> below group those failures by cause. Nothing here yet? Create a <a href="#/connections/new">connection</a> (the <code>demo</code> engine needs no database) or take the tour from the <i class="bi bi-question-circle"></i> menu.
        </help-hint>
        <div class="kpi-grid mb-3" data-tour="dashboard-kpis">
            <kpi-card label="Pipelines" :value="pipelinesRunning + ' / ' + pipelinesTotal" sub="running / defined" icon="bi-diagram-3" :tone="pipelinesRunning ? 'success' : 'muted'"></kpi-card>
            <kpi-card label="Applied (24h)" :value="fmt.num(stats.applied)" icon="bi-check2-all" tone="success"></kpi-card>
            <kpi-card label="Failed (24h)" :value="fmt.num(stats.failed)" icon="bi-x-octagon" :tone="stats.failed ? 'danger' : 'success'"></kpi-card>
            <kpi-card label="Success rate" :value="(stats.success_rate ?? 100) + '%'" :tone="(stats.success_rate ?? 100) < 99 ? 'warning' : 'success'" icon="bi-percent"></kpi-card>
            <kpi-card label="Worst lag" :value="worstLag == null ? '–' : fmt.secs(worstLag)" icon="bi-hourglass-split" :tone="worstLag > 300 ? 'danger' : (worstLag > 60 ? 'warning' : 'info')"></kpi-card>
            <kpi-card label="Dead-letter queue" :value="fmt.num(unresolved)" sub="unresolved failures" icon="bi-envelope-exclamation" :tone="unresolved ? 'warning' : 'success'"></kpi-card>
            <kpi-card label="Active alerts" :value="store.alerts.active.length" icon="bi-bell" :tone="store.alerts.active.length ? 'danger' : 'success'"></kpi-card>
            <kpi-card label="Snapshots" :value="activeSnapshots.length" sub="running / pending" icon="bi-layers" tone="info"></kpi-card>
        </div>

        <div class="row g-3">
            <div class="col-xl-8">
                <div class="card mb-3">
                    <div class="card-header"><span><i class="bi bi-activity me-1"></i> Throughput (events / s)</span>
                        <select v-model.number="range" class="form-select form-select-sm w-auto" @change="loadHistory"><option :value="15">15 min</option><option :value="60">1 h</option><option :value="240">4 h</option></select></div>
                    <div class="card-body"><line-chart :series="throughput" unit="/s"></line-chart></div>
                </div>
                <div class="row g-3">
                    <div class="col-md-6"><div class="card"><div class="card-header"><span><i class="bi bi-hourglass me-1"></i> Replication lag (s)</span></div><div class="card-body"><line-chart :series="lagSeries" unit="s"></line-chart></div></div></div>
                    <div class="col-md-6"><div class="card"><div class="card-header"><span><i class="bi bi-stack me-1"></i> Apply queue depth</span></div><div class="card-body"><line-chart :series="queueSeries"></line-chart></div></div></div>
                </div>
                <div class="card mt-3">
                    <div class="card-header"><span><i class="bi bi-diagram-3 me-1"></i> Pipelines</span><a href="#/pipelines" class="btn btn-sm btn-outline-primary">Manage</a></div>
                    <div class="card-body p-0">
                        <table class="table mb-0" v-if="runtimePipelines.length">
                            <thead><tr><th>Pipeline</th><th>State</th><th>Engine</th><th class="text-end">Read</th><th class="text-end">Applied</th><th class="text-end">Failed</th><th class="text-end">Queue</th><th class="text-end">Lag</th><th>Last event</th></tr></thead>
                            <tbody><tr v-for="p in runtimePipelines" :key="p.id" class="row-clickable" @click="location.hash = '#/pipelines/' + p.id">
                                <td><strong>{{ p.id }}</strong></td><td><state-badge :state="p.state"></state-badge></td><td>{{ p.engine }}</td>
                                <td class="text-end">{{ fmt.num(p.events_read) }}</td><td class="text-end">{{ fmt.num(p.events_applied) }}</td>
                                <td class="text-end" :class="{ 'text-danger fw-bold': p.events_failed }">{{ fmt.num(p.events_failed) }}</td>
                                <td class="text-end">{{ fmt.num(p.queue_depth) }}</td><td class="text-end">{{ p.lag_seconds == null ? '–' : fmt.secs(p.lag_seconds) }}</td><td class="muted">{{ fmt.ago(p.last_event_at) }}</td>
                            </tr></tbody>
                        </table>
                        <empty-state v-else icon="bi-diagram-3" text="No pipeline running."><div class="mt-2"><a href="#/pipelines" class="btn btn-sm btn-primary">Define a pipeline</a></div></empty-state>
                    </div>
                </div>
            </div>
            <div class="col-xl-4">
                <div class="card mb-3">
                    <div class="card-header"><span><i class="bi bi-bell me-1"></i> Active alerts</span><a href="#/alerts" class="small">all</a></div>
                    <div class="card-body">
                        <div v-if="!store.alerts.active.length" class="muted">No active alerts.</div>
                        <div v-for="a in store.alerts.active" :key="a.rule_id + a.scope_key" class="sev-bar mb-2" :class="'sev-' + a.severity">
                            <div><state-badge :state="a.severity"></state-badge> <strong>{{ a.name }}</strong></div>
                            <div class="small muted">{{ a.scope_key }} · since {{ fmt.ago(a.first_seen_iso) }}</div>
                            <div class="small">{{ a.message }}</div>
                        </div>
                    </div>
                </div>
                <div class="card mb-3">
                    <div class="card-header"><span><i class="bi bi-exclamation-octagon me-1"></i> Top failure patterns (24h)</span><a href="#/failures" class="small">all</a></div>
                    <div class="card-body">
                        <div v-if="!patterns.length" class="muted">No failures. 🎉</div>
                        <div v-for="p in patterns" :key="p.signature" class="mb-2 small">
                            <span class="state-badge" :class="{ 'state-warning': p.category === 'fixable', 'state-failed': p.category === 'schema_drift', 'state-stopped': p.category === 'deprecated', 'state-info': p.category === 'transient', 'state-pending': p.category === 'ordering' }">{{ p.category }}</span>
                            <strong>{{ fmt.num(p.count) }}×</strong> {{ p.dest_table }} {{ p.operation }}
                            <div class="mono muted text-truncate">{{ p.sample_error }}</div>
                        </div>
                    </div>
                </div>
                <div class="card mb-3">
                    <div class="card-header"><span><i class="bi bi-broadcast me-1"></i> Live events</span><span class="status-pill" :class="store.ws.connected ? 'ok' : 'off'">{{ store.ws.events.length }}</span></div>
                    <div class="card-body live-feed">
                        <div v-if="!store.ws.events.length" class="muted">Waiting for events…</div>
                        <div v-for="(e, i) in store.ws.events.slice(0, 60)" :key="i" class="ev">
                            <span class="op" :class="'op-' + e.operation">{{ e.operation }}</span>
                            <span class="mono flex-grow-1 text-truncate">{{ e.source_table }}</span>
                            <span class="state-badge" :class="'state-' + e.status">{{ e.status }}</span>
                        </div>
                    </div>
                </div>
                <div class="card">
                    <div class="card-header"><span><i class="bi bi-journal-text me-1"></i> Recent audit</span><a href="#/audit" class="small">all</a></div>
                    <div class="card-body">
                        <div v-for="r in audit" :key="r.id" class="small sev-bar mb-1" :class="'sev-' + r.severity">
                            <span class="muted">{{ fmt.ago(r.ts) }}</span> <span class="badge bg-secondary">{{ r.category }}</span> {{ r.message }}
                        </div>
                        <div v-if="!audit.length" class="muted">No audit records yet.</div>
                    </div>
                </div>
            </div>
        </div>
    </div>`,
};

import { api } from '../api.js';
import { store, fmt } from '../store.js';

export const Metrics = {
    data() { return { store, fmt, raw: null, history: {}, range: 60, q: '', timer: null }; },
    computed: {
        rows() {
            if (!this.raw) return [];
            const out = [];
            for (const [name, def] of Object.entries(this.raw.metrics)) {
                for (const s of def.series) {
                    const labels = Object.entries(s.labels).map(([k, v]) => `${k}=${v}`).join(', ');
                    const value = def.type === 'histogram' ? `n=${fmt.num(s.count)} avg=${fmt.num(s.avg, 4)} p50=${fmt.num(s.p50, 4)} p95=${fmt.num(s.p95, 4)} p99=${fmt.num(s.p99, 4)} max=${fmt.num(s.max, 4)}` : fmt.num(s.value, 3);
                    out.push({ id: name + labels, name, type: def.type, labels, value, description: def.description });
                }
            }
            const q = this.q.toLowerCase();
            return q ? out.filter(r => (r.name + r.labels).toLowerCase().includes(q)) : out;
        },
        columns() { return [{ key: 'name', label: 'Metric', sortable: true }, { key: 'type', label: 'Type' }, { key: 'labels', label: 'Labels' }, { key: 'value', label: 'Value' }]; },
    },
    async created() { await this.load(); this.timer = setInterval(() => this.load(), 10000); },
    unmounted() { clearInterval(this.timer); },
    methods: {
        s(prefix) { return Object.entries(this.history).filter(([k]) => k.startsWith(prefix)).map(([k, v]) => ({ label: k.slice(prefix.length) || k, data: v })); },
        p95ms(name) { const m = this.raw && this.raw.metrics[name]; if (!m || !m.series.length) return null; return Math.max(...m.series.map(s => s.p95)) * 1000; },
        sumCounter(name) { const m = this.raw && this.raw.metrics[name]; return m ? m.series.reduce((a, s) => a + (s.value || 0), 0) : 0; },
        async load() { try { const [raw, h] = await Promise.all([api.metrics.json(), api.metrics.history({ minutes: this.range })]); this.raw = raw; this.history = h.series; } catch (e) { /* ignore */ } },
    },
    template: `<div>
        <help-hint id="metrics" title="Performance counters">
            Prometheus-style counters and histograms, sampled every few seconds into a short in-memory history for the charts. <em>Source round-trips</em> and <em>watermark checks</em> tell you how much load the readers put on the sources (push mode keeps them near zero while idle);
            <em>apply latency</em> is the time per batch at the destination; <em>lag</em> is source commit time → applied. Scrape <code>/metrics</code> with Prometheus for long-term retention and dashboards.
        </help-hint>
        <div class="kpi-grid mb-3" v-if="raw" data-tour="metrics-kpis">
            <kpi-card label="Events read" :value="fmt.num(sumCounter('cdc_events_read_total'))" tone="info"></kpi-card>
            <kpi-card label="Events applied" :value="fmt.num(sumCounter('cdc_events_applied_total'))" tone="success"></kpi-card>
            <kpi-card label="Events failed" :value="fmt.num(sumCounter('cdc_events_failed_total'))" :tone="sumCounter('cdc_events_failed_total') ? 'danger' : 'success'"></kpi-card>
            <kpi-card label="Source round-trips" :value="fmt.num(sumCounter('source_roundtrips_total'))" sub="all queries to sources" tone="muted"></kpi-card>
            <kpi-card label="Watermark checks" :value="fmt.num(sumCounter('watermark_checks_total'))" sub="cheap idle polls" tone="muted"></kpi-card>
            <kpi-card label="Apply latency p95" :value="p95ms('apply_latency_seconds') == null ? '–' : fmt.num(p95ms('apply_latency_seconds'), 1) + ' ms'" tone="info"></kpi-card>
            <kpi-card label="Snapshot rows" :value="fmt.num(sumCounter('snapshot_rows_total'))" tone="info"></kpi-card>
            <kpi-card label="Alerts fired" :value="fmt.num(sumCounter('alerts_fired_total'))" tone="warning"></kpi-card>
        </div>
        <div class="toolbar"><select v-model.number="range" class="form-select form-select-sm" @change="load()"><option :value="15">15 min</option><option :value="60">1 h</option><option :value="240">4 h</option></select><a class="btn btn-sm btn-outline-secondary ms-auto" href="/metrics" target="_blank"><i class="bi bi-box-arrow-up-right"></i> Prometheus exposition</a></div>
        <div class="row g-3 mb-3">
            <div class="col-lg-6"><div class="card"><div class="card-header"><span>Throughput (events/s)</span></div><div class="card-body"><line-chart :series="[{label:'applied', data: history['rate.applied'] || [], color:'#10b981'},{label:'failed', data: history['rate.failed'] || [], color:'#ef4444'},{label:'read', data: history['rate.read'] || [], color:'#2563eb'}]" unit="/s"></line-chart></div></div></div>
            <div class="col-lg-6"><div class="card"><div class="card-header"><span>Source round-trips / s</span></div><div class="card-body"><line-chart :series="[{label:'round-trips', data: history['rate.roundtrips'] || []}]" unit="/s"></line-chart></div></div></div>
            <div class="col-lg-4"><div class="card"><div class="card-header"><span>Replication lag (s)</span></div><div class="card-body"><line-chart :series="s('lag.')" unit="s"></line-chart></div></div></div>
            <div class="col-lg-4"><div class="card"><div class="card-header"><span>Apply latency p95 (ms)</span></div><div class="card-body"><line-chart :series="s('apply_p95.')" unit="ms"></line-chart></div></div></div>
            <div class="col-lg-4"><div class="card"><div class="card-header"><span>Queue depth</span></div><div class="card-body"><line-chart :series="s('queue.')"></line-chart></div></div></div>
            <div class="col-lg-6"><div class="card"><div class="card-header"><span>Snapshot rows / s</span></div><div class="card-body"><line-chart :series="[{label:'rows/s', data: history['rate.snapshot_rows'] || [], color:'#8b5cf6'}]" unit="/s"></line-chart></div></div></div>
        </div>
        <div class="card"><div class="card-header"><span>All counters</span><input v-model="q" class="form-control form-control-sm w-auto" placeholder="filter…"></div>
            <div class="card-body"><data-table :columns="columns" :rows="rows" :page-size="40" :searchable="false" dense>
                <template #cell-name="{ row }"><span class="mono">{{ row.name }}</span><div class="small muted">{{ row.description }}</div></template>
                <template #cell-labels="{ row }"><span class="small muted">{{ row.labels }}</span></template>
                <template #cell-value="{ row }"><span class="mono">{{ row.value }}</span></template>
            </data-table></div></div>
    </div>`,
};

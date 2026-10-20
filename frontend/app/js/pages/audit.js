import { api } from '../api.js';
import { store, fmt, download } from '../store.js';

export const Audit = {
    data() { return { store, fmt, rows: [], filters: { category: '', severity: '', table: '', hours: 24, status: '' }, q: '', limit: 500, busy: false, detail: null }; },
    computed: {
        filtered() { const q = this.q.toLowerCase(); return q ? this.rows.filter(r => JSON.stringify(r).toLowerCase().includes(q)) : this.rows; },
        columns() { return [
            { key: 'ts', label: 'Time', sortable: true, format: v => fmt.ts(v) }, { key: 'category', label: 'Category', sortable: true }, { key: 'severity', label: 'Severity', sortable: true },
            { key: 'component', label: 'Component' }, { key: 'table_key', label: 'Table', sortable: true }, { key: 'message', label: 'Message' }, { key: 'actions', label: '' },
        ]; },
    },
    async created() { await this.load(); },
    methods: {
        async load() { this.busy = true; try { this.rows = await api.audit.events({ ...this.filters, limit: this.limit }); } catch (e) { store.error(e.message); } finally { this.busy = false; } },
        exportCsv() {
            const cols = ['id', 'ts', 'category', 'severity', 'component', 'source', 'table_key', 'operation', 'status', 'message', 'signature', 'count'];
            const esc = v => `"${String(v ?? '').replace(/"/g, '""')}"`;
            download(`audit-${new Date().toISOString().slice(0, 19)}.csv`, [cols.join(','), ...this.filtered.map(r => cols.map(c => esc(r[c])).join(','))].join('\n'), 'text/csv');
        },
    },
    template: `<div>
        <help-hint id="audit" title="Audit trail">
            Append-only record of everything the service did: pipeline lifecycle, per-batch results, snapshot chunks, configuration changes (who changed which document, with the diff), prerequisite checks, alerts.
            Set <code>CDCSYNC_AUDIT_LEVEL=all</code> to also keep one row per change event (costly on busy sources). Filter by category, severity, table or time, search the text, and export CSV for auditors. Retention is configured under Settings.
        </help-hint>
        <div class="toolbar" data-tour="audit-filters">
            <select v-model="filters.category" class="form-select form-select-sm" @change="load()"><option value="">all categories</option><option v-for="c in ['pipeline','batch','snapshot','config','alert','warning','prerequisite','cdc_event','failure']" :key="c" :value="c">{{ c }}</option></select>
            <select v-model="filters.severity" class="form-select form-select-sm" @change="load()"><option value="">all severities</option><option v-for="s in ['info','warning','error','critical']" :key="s" :value="s">{{ s }}</option></select>
            <input v-model="filters.table" class="form-control form-control-sm mono" placeholder="table (schema.table)" @change="load()">
            <select v-model.number="filters.hours" class="form-select form-select-sm" @change="load()"><option :value="1">1 h</option><option :value="24">24 h</option><option :value="168">7 d</option><option :value="720">30 d</option></select>
            <input v-model="q" class="form-control form-control-sm" placeholder="search text…">
            <button class="btn btn-sm btn-outline-secondary ms-auto" @click="exportCsv()"><i class="bi bi-download"></i> CSV</button>
            <button class="btn btn-sm btn-outline-primary" @click="load()"><i class="bi bi-arrow-clockwise"></i></button>
        </div>
        <div class="card"><div class="card-body">
            <data-table :columns="columns" :rows="filtered" :page-size="50" :searchable="false" dense empty-text="No audit records for these filters.">
                <template #cell-category="{ row }"><span class="badge bg-secondary">{{ row.category }}</span></template>
                <template #cell-severity="{ row }"><state-badge :state="row.severity"></state-badge></template>
                <template #cell-table_key="{ row }"><span class="mono">{{ row.table_key || '' }}</span></template>
                <template #cell-message="{ row }"><span>{{ row.message }}</span><span v-if="row.status" class="badge ms-1" :class="{ 'bg-success': row.status === 'ok', 'bg-warning text-dark': row.status === 'partial', 'bg-danger': row.status === 'failed' }">{{ row.status }}</span></template>
                <template #cell-actions="{ row }"><button v-if="row.payload || row.position" class="btn btn-sm btn-link p-0" @click="detail = row"><i class="bi bi-braces"></i></button></template>
            </data-table>
        </div></div>
        <modal-dialog v-if="detail" :title="'Audit record #' + detail.id" size="lg" @close="detail = null"><json-view :value="detail" max-height="520px"></json-view></modal-dialog>
    </div>`,
};

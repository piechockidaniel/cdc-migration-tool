import { api } from '../api.js';
import { store, fmt } from '../store.js';

const CATEGORY_CLASS = { fixable: 'state-warning', schema_drift: 'state-failed', deprecated: 'state-stopped', transient: 'state-info', ordering: 'state-pending', unknown: 'state-stopped' };
const MANUAL_ACTIONS = new Set(['widen_type', 'rescan_columns', 'grant_privileges', 'reorder_mappings']);

export const Failures = {
    data() {
        return { store, fmt, CATEGORY_CLASS, tab: 'patterns', hours: 24, patterns: [], summary: null, dlq: [], selected: [], detail: null, replayPipeline: '',
                 includeResolved: false, categoryFilter: '', applying: null, busy: false };
    },
    computed: {
        runningPipelines() { return store.runningPipelines(); },
        filteredPatterns() { return this.patterns.filter(p => !this.categoryFilter || p.category === this.categoryFilter); },
        dlqColumns() { return [
            { key: 'sel', label: '' }, { key: 'id', label: '#', sortable: true }, { key: 'ts', label: 'Time', sortable: true, format: v => fmt.ts(v) },
            { key: 'operation', label: 'Op' }, { key: 'source_table', label: 'Source → destination' }, { key: 'error', label: 'Error' }, { key: 'attempts', label: 'Attempts', class: 'text-end' }, { key: 'actions', label: '' },
        ]; },
    },
    async created() { await this.load(); if (this.runningPipelines.length === 1) this.replayPipeline = this.runningPipelines[0].id; },
    methods: {
        async load() {
            try {
                const [p, d] = await Promise.all([api.audit.patterns({ hours: this.hours, include_resolved: this.includeResolved }), api.audit.failures({ resolved: this.includeResolved ? null : false, limit: 500 })]);
                this.patterns = p.patterns; this.summary = p.summary; this.dlq = d; this.selected = [];
            } catch (e) { store.error(e.message); }
        },
        needsPipeline() { if (!this.replayPipeline) { store.warn('Select a running pipeline to replay through (its applier and destination are used).'); return false; } return true; },
        async replaySignature(p) { if (!this.needsPipeline()) return; const r = await store.run(() => api.audit.replay({ signature: p.signature, pipeline_id: this.replayPipeline })); if (r) { (r.still_failing ? store.warn : store.success).call(store, `replayed ${r.replayed}, still failing ${r.still_failing}`); await this.load(); await store.loadRuntime(); } },
        async replayIds(ids) { if (!this.needsPipeline()) return; const r = await store.run(() => api.audit.replay({ ids, pipeline_id: this.replayPipeline })); if (r) { (r.still_failing ? store.warn : store.success).call(store, `replayed ${r.replayed}, still failing ${r.still_failing}`); await this.load(); await store.loadRuntime(); } },
        async resolveSignature(p) { if (!await store.ask(`Mark ${p.count} failures of this pattern as resolved (they will not be replayed)?`, { okLabel: 'Resolve' })) return; const r = await store.run(() => api.audit.resolve({ signature: p.signature, resolution: 'manual (UI)' })); if (r) { store.success(`${r.resolved} resolved`); await this.load(); await store.loadRuntime(); } },
        async resolveIds(ids) { if (!ids.length) return; const r = await store.run(() => api.audit.resolve({ ids, resolution: 'manual (UI)' })); if (r) { store.success(`${r.resolved} resolved`); await this.load(); await store.loadRuntime(); } },
        async openDetail(id) { this.detail = await store.run(() => api.audit.failure(id)); },
        toggleAll(ev) { this.selected = ev.target.checked ? this.dlq.map(f => f.id) : []; },
        startApply(p) {
            const action = p.proposed_change && p.proposed_change.action;
            if (!action || action === 'replay') return;
            if (MANUAL_ACTIONS.has(action)) { store.warn(`"${action}" must be done on the database / mapping by hand: ${p.suggestion}`); return; }
            const candidates = store.mappings.filter(m => `${m.destination_schema}.${m.destination_table}` === p.dest_table);
            this.applying = { pattern: p, action, mapping_id: candidates.length === 1 ? candidates[0].id : '', params: { value: '', column: '', expression: '', key_columns: '' }, candidates };
        },
        async confirmApply() {
            const a = this.applying; const params = {};
            if (a.action === 'set_default_value') params.value = a.params.value;
            if (a.action === 'add_transformation') { params.column = a.params.column; params.expression = a.params.expression; }
            if (a.action === 'set_key_columns') params.key_columns = a.params.key_columns.split(',').map(s => s.trim()).filter(Boolean);
            if (a.action === 'set_sync_mode') params.value = 'upsert';
            const r = await store.run(() => api.audit.applyPattern({ signature: a.pattern.signature, action: a.action, mapping_id: a.mapping_id || null, params }));
            if (r) { store.success(`applied: ${JSON.stringify(r.change)} – ${r.note}`); this.applying = null; await Promise.all([this.load(), store.loadReference()]); }
        },
    },
    template: `<div>
        <help-hint id="failures" title="Failures - learn from what did not apply">
            Rows that fail to apply are parked in the <strong>dead-letter queue</strong> with the event, the destination error and the position; the stream keeps flowing.
            The <strong>pattern miner</strong> groups failures by normalised error signature (values and ids stripped) and proposes a remedy: widen a column, add a default, map a renamed field, or mark a legacy table as deprecated and filter it out.
            Fix the mapping or the schema, pick a running pipeline and <strong>replay</strong> the affected rows; <strong>resolve</strong> the rest with a note so the audit trail explains why they were dropped.
        </help-hint>
        <div class="toolbar">
            <ul class="nav tabs-clean mb-0" data-tour="failures-tabs">
                <li class="nav-item"><a class="nav-link" :class="{ active: tab === 'patterns' }" href="#" @click.prevent="tab = 'patterns'"><i class="bi bi-diagram-2"></i> Patterns ({{ patterns.length }})</a></li>
                <li class="nav-item"><a class="nav-link" :class="{ active: tab === 'dlq' }" href="#" @click.prevent="tab = 'dlq'"><i class="bi bi-envelope-exclamation"></i> Dead-letter queue ({{ dlq.length }})</a></li>
            </ul>
            <select v-model.number="hours" class="form-select form-select-sm ms-auto" @change="load()"><option :value="1">1 h</option><option :value="24">24 h</option><option :value="168">7 d</option><option :value="720">30 d</option></select>
            <div class="form-check form-switch mb-0"><input v-model="includeResolved" class="form-check-input" type="checkbox" id="inc-res" @change="load()"><label class="form-check-label small" for="inc-res">include resolved</label></div>
            <select v-model="replayPipeline" class="form-select form-select-sm" title="pipeline used for replays"><option value="">replay via pipeline…</option><option v-for="p in runningPipelines" :key="p.id" :value="p.id">{{ p.id }} ({{ p.state }})</option></select>
        </div>

        <div v-if="summary" class="kpi-grid mb-3">
            <kpi-card label="Failures" :value="fmt.num(summary.failures)" :sub="summary.patterns + ' patterns'" :tone="summary.failures ? 'danger' : 'success'"></kpi-card>
            <kpi-card v-for="(n, c) in summary.by_category" :key="c" :label="c" :value="fmt.num(n)" :tone="c === 'transient' ? 'info' : (c === 'deprecated' ? 'muted' : 'warning')"></kpi-card>
            <kpi-card label="Replayable" :value="fmt.num(summary.replayable)" sub="transient errors" tone="info"></kpi-card>
            <kpi-card label="Systemic" :value="summary.systemic.length" sub="recurring ≥ 6h" :tone="summary.systemic.length ? 'danger' : 'success'"></kpi-card>
        </div>
        <div v-if="summary && summary.deprecated_candidates.length" class="alert alert-warning py-2 small"><i class="bi bi-signpost-split"></i> Deprecation candidates (objects that no longer exist): <strong>{{ summary.deprecated_candidates.join(', ') }}</strong> – consider removing these mappings from the next iteration.</div>

        <div v-if="tab === 'patterns'">
            <div class="toolbar"><span class="small muted">category:</span><button v-for="c in ['', 'fixable', 'schema_drift', 'deprecated', 'transient', 'ordering', 'unknown']" :key="c" class="btn btn-sm" :class="categoryFilter === c ? 'btn-dark' : 'btn-outline-secondary'" @click="categoryFilter = c">{{ c || 'all' }}</button></div>
            <empty-state v-if="!filteredPatterns.length" icon="bi-emoji-sunglasses" text="No failure patterns in this window."></empty-state>
            <div v-for="p in filteredPatterns" :key="p.signature" class="pattern">
                <div class="d-flex align-items-center gap-2 flex-wrap">
                    <span class="state-badge" :class="CATEGORY_CLASS[p.category]">{{ p.category }}</span>
                    <strong>{{ fmt.num(p.count) }}×</strong> <span class="mono">{{ p.dest_table }}</span> <span class="badge bg-secondary">{{ p.operation }}</span>
                    <span v-if="p.systemic" class="badge bg-danger">systemic</span>
                    <span class="muted small ms-auto">{{ Math.round(p.share * 100) }}% of failures · first {{ fmt.ago(p.first_seen) }} · last {{ fmt.ago(p.last_seen) }}</span>
                </div>
                <div class="mono small text-danger mt-1">{{ p.sample_error }}</div>
                <div class="small mt-1"><i class="bi bi-lightbulb text-warning"></i> {{ p.suggestion }}</div>
                <div class="mt-2 d-flex gap-2 flex-wrap">
                    <button v-if="p.category === 'transient' || p.proposed_change.action === 'replay'" class="btn btn-sm btn-outline-primary" @click="replaySignature(p)"><i class="bi bi-arrow-repeat"></i> Replay {{ p.count }}</button>
                    <button v-if="p.proposed_change && p.proposed_change.action && p.proposed_change.action !== 'replay'" class="btn btn-sm btn-outline-success" @click="startApply(p)"><i class="bi bi-magic"></i> Apply: {{ p.proposed_change.action }}</button>
                    <button class="btn btn-sm btn-outline-secondary" @click="resolveSignature(p)"><i class="bi bi-check2"></i> Mark resolved</button>
                    <button class="btn btn-sm btn-link" @click="tab = 'dlq'">show rows</button>
                </div>
            </div>
        </div>

        <div v-if="tab === 'dlq'" class="card"><div class="card-body">
            <div class="toolbar" v-if="selected.length"><span class="small">{{ selected.length }} selected</span>
                <button class="btn btn-sm btn-outline-primary" @click="replayIds(selected)"><i class="bi bi-arrow-repeat"></i> Replay</button>
                <button class="btn btn-sm btn-outline-secondary" @click="resolveIds(selected)"><i class="bi bi-check2"></i> Resolve</button></div>
            <data-table :columns="dlqColumns" :rows="dlq" :page-size="50" dense empty-text="Dead-letter queue is empty.">
                <template #cell-sel="{ row }"><input type="checkbox" class="form-check-input" :value="row.id" v-model="selected"></template>
                <template #cell-operation="{ row }"><span :class="'op-' + row.operation">{{ row.operation }}</span></template>
                <template #cell-source_table="{ row }"><span class="mono">{{ row.source_table }}</span> → <span class="mono">{{ row.dest_table }}</span><span v-if="row.resolved" class="badge bg-success ms-1">resolved</span></template>
                <template #cell-error="{ row }"><span class="mono small text-danger d-inline-block text-truncate" style="max-width: 380px" :title="row.error">{{ row.error }}</span></template>
                <template #cell-actions="{ row }"><div class="btn-group btn-group-sm"><button class="btn btn-outline-secondary" @click="openDetail(row.id)" title="payload"><i class="bi bi-braces"></i></button><button class="btn btn-outline-primary" @click="replayIds([row.id])" title="replay"><i class="bi bi-arrow-repeat"></i></button><button class="btn btn-outline-secondary" @click="resolveIds([row.id])" title="resolve"><i class="bi bi-check2"></i></button></div></template>
            </data-table>
        </div></div>

        <modal-dialog v-if="detail" :title="'Failure #' + detail.id" size="lg" @close="detail = null">
            <dl class="kv mb-2"><dt>error</dt><dd class="mono text-danger">{{ detail.error }}</dd><dt>signature</dt><dd class="mono">{{ detail.signature }}</dd><dt>attempts</dt><dd>{{ detail.attempts }}</dd><dt>resolution</dt><dd>{{ detail.resolution || '–' }}</dd></dl>
            <json-view :value="detail.payload" max-height="420px"></json-view>
        </modal-dialog>

        <modal-dialog v-if="applying" :title="'Apply ' + applying.action" @close="applying = null">
            <p class="small">{{ applying.pattern.suggestion }}</p>
            <div class="mb-2"><label class="form-label">Mapping</label>
                <select v-model="applying.mapping_id" class="form-select"><option value="">auto-detect from pattern</option><option v-for="m in applying.candidates.length ? applying.candidates : store.mappings" :key="m.id" :value="m.id">{{ m.id }} ({{ m.destination_schema }}.{{ m.destination_table }})</option></select></div>
            <div v-if="applying.action === 'set_default_value'"><label class="form-label">Default value for the NULL column</label><input v-model="applying.params.value" class="form-control"></div>
            <div v-if="applying.action === 'add_transformation'" class="row g-2"><div class="col-5"><label class="form-label">Destination column</label><input v-model="applying.params.column" class="form-control mono"></div><div class="col-7"><label class="form-label">DuckDB expression</label><input v-model="applying.params.expression" class="form-control mono" placeholder='LEFT("Name", 50)'></div></div>
            <div v-if="applying.action === 'set_key_columns'"><label class="form-label">Key columns (destination, comma separated)</label><input v-model="applying.params.key_columns" class="form-control mono"></div>
            <div v-if="applying.action === 'disable_mapping'" class="alert alert-warning small mb-0">The mapping will be disabled (removed from the next pipeline start) and the pattern's failures marked resolved.</div>
            <div v-if="applying.action === 'set_sync_mode'" class="alert alert-info small mb-0">sync_mode will be set to <code>upsert</code>.</div>
            <template #footer><button class="btn btn-secondary" @click="applying = null">Cancel</button><button class="btn btn-success" @click="confirmApply()">Apply</button></template>
        </modal-dialog>
    </div>`,
};

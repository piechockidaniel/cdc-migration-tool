import { api } from '../api.js';
import { store, fmt } from '../store.js';

function emptyMapping() {
    return {
        id: '', name: '', mapping_type: 'table', enabled: true, source_schema: '', source_table: '', destination_schema: '', destination_table: '',
        column_mappings: [], key_columns: [], sync_inserts: true, sync_updates: true, sync_deletes: true, sync_mode: 'upsert', batch_size: 1000,
        timeout_seconds: 300, perform_initial_snapshot: false, source_query: '', assigned_worksets: [], is_multi_source: false, sources: [],
    };
}
const emptyColumn = () => ({ source_column: '', destination_column: '', transformation: '', transformation_engine: 'duckdb', default_value: null, ignore_changes: false, auto_generate: 'none', auto_generate_expression: '' });

export const Mappings = {
    props: ['params'],
    data() {
        return {
            store, fmt, editing: null, editMode: false, validation: null, busy: false,
            helper: { srcConn: '', dstConn: '', srcTables: [], dstTables: [], srcCols: [], dstCols: [], loading: false },
            history: null, testResult: null, filterType: '',
        };
    },
    computed: {
        rows() {
            return store.mappings.filter(m => !this.filterType || m.mapping_type === this.filterType).map(m => ({
                ...m, source: m.mapping_type === 'sql' ? '(query)' : `${m.source_schema}.${m.source_table}`,
                destination: `${m.destination_schema}.${m.destination_table}`, keys: (m.key_columns || []).join(', ') || '(auto)',
                cols: (m.column_mappings || []).length, snapshot: m.perform_initial_snapshot ? (m.snapshot_completed_at ? 'done ' + fmt.ago(m.snapshot_completed_at) : 'pending') : '–',
            }));
        },
        columns() { return [
            { key: 'id', label: 'Mapping', sortable: true }, { key: 'mapping_type', label: 'Type', sortable: true }, { key: 'source', label: 'Source', sortable: true },
            { key: 'destination', label: 'Destination', sortable: true }, { key: 'cols', label: 'Columns', class: 'text-end' }, { key: 'keys', label: 'Keys' },
            { key: 'snapshot', label: 'Snapshot' }, { key: 'enabled', label: 'Enabled' }, { key: 'actions', label: '', class: 'text-end' },
        ]; },
        destColumnNames() { return this.helper.dstCols.map(c => c.name); },
        sourceColumnNames() { return this.helper.srcCols.map(c => c.name); },
        sqlConnections() { return store.connections.filter(c => (c.engine || 'mssql') !== 'mongodb'); },
    },
    created() { if (this.params?.id === 'new') this.openNew(); else if (this.params?.id) { const m = store.mappingById(this.params.id); if (m) this.openEdit(m); } },
    methods: {
        openNew() { this.editing = emptyMapping(); this.editMode = false; this.validation = null; this.resetHelper(); },
        openEdit(m) { this.editing = JSON.parse(JSON.stringify({ ...emptyMapping(), ...m })); this.editing.column_mappings = (this.editing.column_mappings || []).map(c => ({ ...emptyColumn(), ...c })); this.editMode = true; this.validation = null; this.resetHelper(); },
        duplicate(m) { this.openEdit(m); this.editing.id = m.id + '_copy'; this.editing.name = m.name + ' (copy)'; this.editing.snapshot_completed_at = null; this.editMode = false; },
        resetHelper() { this.helper = { srcConn: '', dstConn: '', srcTables: [], dstTables: [], srcCols: [], dstCols: [], loading: false }; },
        async loadTables(side) {
            const connId = side === 'src' ? this.helper.srcConn : this.helper.dstConn;
            if (!connId) return;
            this.helper.loading = true;
            try { this.helper[side + 'Tables'] = await api.connectors.tables({ connection_id: connId }); }
            catch (e) { store.error(e.message); } finally { this.helper.loading = false; }
        },
        async loadColumns(side) {
            const connId = side === 'src' ? this.helper.srcConn : this.helper.dstConn;
            const key = side === 'src' ? `${this.editing.source_schema}.${this.editing.source_table}` : `${this.editing.destination_schema}.${this.editing.destination_table}`;
            if (!connId || key === '.') return;
            this.helper.loading = true;
            try { const r = await api.connectors.columns({ connection_id: connId, tables: [key] }); this.helper[side + 'Cols'] = r[key] || []; }
            catch (e) { store.error(e.message); } finally { this.helper.loading = false; }
        },
        pickTable(side, key) {
            const [schema, table] = key.split('.');
            if (side === 'src') { this.editing.source_schema = schema; this.editing.source_table = table; if (!this.editing.id) this.editing.id = `${schema}_${table}`.toLowerCase(); if (!this.editing.name) this.editing.name = key; }
            else { this.editing.destination_schema = schema; this.editing.destination_table = table; }
            this.loadColumns(side);
        },
        autoMap() {
            const norm = (n) => n.toLowerCase().replace(/[^a-z0-9]/g, '');
            const dst = new Map(this.destColumnNames.map(n => [norm(n), n]));
            const usedDst = new Set(this.editing.column_mappings.map(c => c.destination_column).filter(Boolean));
            const existing = new Set(this.editing.column_mappings.map(c => c.source_column));
            let matched = 0, unmatched = 0;
            const srcPk = this.helper.srcCols.filter(c => c.is_primary_key).map(c => c.name);
            const dstPk = this.helper.dstCols.filter(c => c.is_primary_key).map(c => c.name).filter(n => !usedDst.has(n));
            for (const s of this.sourceColumnNames) {
                if (existing.has(s)) continue;
                let d = dst.get(norm(s));
                if (d && usedDst.has(d)) d = null;
                if (!d && srcPk.includes(s) && dstPk.length) d = dstPk.shift();          // PK -> PK when names differ (CustomerID -> id)
                if (!d && !this.destColumnNames.length) d = s;                           // no destination metadata: same name
                this.editing.column_mappings.push({ ...emptyColumn(), source_column: s, destination_column: d || '' });
                if (d) { usedDst.add(d); matched++; } else unmatched++;
            }
            if (!this.editing.key_columns.length) {
                const pk = this.helper.dstCols.filter(c => c.is_primary_key).map(c => c.name);
                if (pk.length) this.editing.key_columns = pk;
                else this.editing.key_columns = srcPk.map(s => (this.editing.column_mappings.find(cm => cm.source_column === s) || {}).destination_column).filter(Boolean);
            }
            (unmatched ? store.warn : store.success).call(store, `${matched} columns mapped${unmatched ? `, ${unmatched} source columns without a destination (fill in or remove)` : ''}`);
        },
        addColumn() { this.editing.column_mappings.push(emptyColumn()); },
        removeColumn(i) { this.editing.column_mappings.splice(i, 1); },
        toggleKey(name) { const i = this.editing.key_columns.indexOf(name); if (i >= 0) this.editing.key_columns.splice(i, 1); else this.editing.key_columns.push(name); },
        payload() {
            const m = JSON.parse(JSON.stringify(this.editing));
            m.id = (m.id || `${m.source_schema}_${m.source_table}`).trim();
            m.name = m.name || m.id;
            m.column_mappings = m.column_mappings.filter(c => c.destination_column).map(c => {
                const out = { ...c };
                for (const k of ['transformation', 'auto_generate_expression', 'default_value', 'source_column']) if (out[k] === '' || out[k] === undefined) out[k] = null;
                if (!out.transformation) out.transformation_type = null; else out.transformation_type = out.transformation_type || 'custom';
                return out;
            });
            if (!m.key_columns || !m.key_columns.length) m.key_columns = null;
            if (m.mapping_type === 'sql') { m.source_schema = null; m.source_table = null; m.column_mappings = []; } else { m.source_query = null; }
            return m;
        },
        async save() {
            const m = this.payload();
            const r = await store.run(() => this.editMode ? api.mappings.update(m) : api.mappings.create(m), { successMessage: 'Mapping saved' });
            if (r) { this.editing = null; await store.loadReference(); }
        },
        async remove(m) {
            if (!await store.ask(`Delete mapping "${m.id}"?`, { danger: true, okLabel: 'Delete' })) return;
            await store.run(() => api.mappings.remove(m.id), { successMessage: 'Mapping deleted' }); await store.loadReference();
        },
        async toggleEnabled(m) { const full = { ...m, enabled: !m.enabled }; await store.run(() => api.mappings.update(full)); await store.loadReference(); },
        async test(m) { this.testResult = { id: m.id, ...(await store.run(() => api.mappings.test(m.id)) || {}) }; },
        async showHistory(m) { this.history = await store.run(() => api.mappings.history(m.id)); },
        async verify(m) {
            const r = await store.run(() => api.mappings.verify({ mapping_id: m.id, sample_size: 100, check_row_counts: true, check_reverse_mapping: true }));
            if (r) this.testResult = { id: m.id, verify: r, success: r.row_count_match && r.samples_matched === r.samples_checked, message: `rows ${fmt.num(r.source_row_count)} vs ${fmt.num(r.destination_row_count)} · ${r.samples_matched}/${r.samples_checked} samples match` };
        },
        async execute(m) {
            if (!await store.ask(`Execute SQL mapping "${m.id}" now (sync_mode=${m.sync_mode}) using the active working set connections?`, { okLabel: 'Execute' })) return;
            const r = await store.run(() => api.mappings.execute(m.id));
            if (r) (r.success ? store.success : store.error).call(store, `${r.message} (${fmt.num(r.rows_processed)} rows)`);
        },
        snapshot(m) { location.hash = `#/snapshots/new?mapping=${encodeURIComponent(m.id)}`; },
    },
    template: `<div>
        <help-hint id="mappings" title="Step 2 - describe where each table goes">
            A <strong>table</strong> mapping is applied change-by-change by a pipeline: source table → destination table, with per-column controls (rename, ignore, default value, auto-generate, a DuckDB expression) and optionally a DuckDB script that runs per batch.
            A <strong>sql</strong> mapping re-executes a query (snapshot-style refresh). Key columns default to the destination primary key; set them explicitly when the destination has none.
            Validate before saving - the validator catches missing columns, unknown destination tables and unsafe expressions. Multi-source merges and the drag-and-drop designer are in the legacy UI.
        </help-hint>
        <div class="toolbar">
            <button class="btn btn-primary btn-sm" @click="openNew()" data-tour="mappings-new"><i class="bi bi-plus-lg"></i> New mapping</button>
            <select v-model="filterType" class="form-select form-select-sm"><option value="">all types</option><option value="table">table (CDC)</option><option value="sql">sql (query)</option></select>
            <a class="btn btn-outline-secondary btn-sm ms-auto" href="/legacy/admin" target="_blank"><i class="bi bi-box-arrow-up-right"></i> Visual workflow designer / multi-source (legacy)</a>
        </div>
        <div class="card"><div class="card-body">
            <data-table :columns="columns" :rows="rows" row-key="id" empty-text="No mappings yet.">
                <template #cell-id="{ row }"><strong>{{ row.id }}</strong><div class="small muted">{{ row.name }}</div></template>
                <template #cell-mapping_type="{ row }"><span class="badge" :class="row.mapping_type === 'table' ? 'bg-primary' : 'bg-info text-dark'">{{ row.mapping_type }}</span><span v-if="row.is_multi_source" class="badge bg-secondary ms-1">multi</span></template>
                <template #cell-source="{ row }"><span class="mono">{{ row.source }}</span></template>
                <template #cell-destination="{ row }"><span class="mono">{{ row.destination }}</span></template>
                <template #cell-keys="{ row }"><span class="mono" :class="{ 'text-warning': row.keys === '(auto)' }">{{ row.keys }}</span></template>
                <template #cell-enabled="{ row }"><div class="form-check form-switch mb-0"><input class="form-check-input" type="checkbox" :checked="row.enabled" @change="toggleEnabled(row)"></div></template>
                <template #cell-actions="{ row }">
                    <div class="btn-group btn-group-sm">
                        <button class="btn btn-outline-secondary" title="Validate / test" @click="test(row)"><i class="bi bi-check2-square"></i></button>
                        <button v-if="row.mapping_type === 'table'" class="btn btn-outline-secondary" title="Verify data quality (row counts + samples)" @click="verify(row)"><i class="bi bi-shield-check"></i></button>
                        <button v-if="row.mapping_type === 'table'" class="btn btn-outline-secondary" title="Snapshot (initial load)" @click="snapshot(row)"><i class="bi bi-layers"></i></button>
                        <button v-if="row.mapping_type === 'sql'" class="btn btn-outline-secondary" title="Execute now" @click="execute(row)"><i class="bi bi-play"></i></button>
                        <button class="btn btn-outline-secondary" title="History" @click="showHistory(row)"><i class="bi bi-clock-history"></i></button>
                        <button class="btn btn-outline-secondary" title="Duplicate" @click="duplicate(row)"><i class="bi bi-files"></i></button>
                        <button class="btn btn-outline-secondary" title="Edit" @click="openEdit(row)"><i class="bi bi-pencil"></i></button>
                        <button class="btn btn-outline-danger" title="Delete" @click="remove(row)"><i class="bi bi-trash"></i></button>
                    </div>
                </template>
            </data-table>
        </div></div>

        <!-- test / verify result -->
        <modal-dialog v-if="testResult" :title="'Result – ' + testResult.id" @close="testResult = null">
            <div class="alert" :class="testResult.success ? 'alert-success' : 'alert-danger'">{{ testResult.message || (testResult.success ? 'OK' : (testResult.error || 'failed')) }}</div>
            <ul v-if="testResult.errors && testResult.errors.length"><li v-for="e in testResult.errors" :key="e">{{ e }}</li></ul>
            <json-view v-if="testResult.verify" :value="testResult.verify"></json-view>
            <json-view v-else-if="testResult.sample_data" :value="testResult.sample_data"></json-view>
        </modal-dialog>

        <modal-dialog v-if="history" :title="'History – ' + history.mapping_id + ' (version ' + history.version + ')'" size="lg" @close="history = null">
            <div v-if="!history.history.length" class="muted">No history (JSON backend or never changed).</div>
            <div v-for="h in history.history" :key="h.version" class="mb-2">
                <div class="small"><span class="badge bg-secondary">v{{ h.version }}</span> {{ h.body._action }} · {{ fmt.ts(h.updated_at) }} · {{ h.updated_by || 'api' }}</div>
                <details><summary class="small muted">document</summary><json-view :value="h.body" max-height="240px"></json-view></details>
            </div>
        </modal-dialog>

        <!-- editor -->
        <modal-dialog v-if="editing" :title="editMode ? 'Edit mapping ' + editing.id : 'New mapping'" size="xl" @close="editing = null">
            <help-hint id="mapping-editor" icon="bi-info-circle">
                <strong>table</strong>: pick source and destination tables, then one row per destination column - a source column, or a <em>default</em> / <em>auto-generate</em> control, or a DuckDB expression over the source row (e.g. <code>concat_ws(' ', "First", "Last")</code>).
                Columns you do not list are ignored. <em>Key columns</em> are only needed when the destination has no primary key. <strong>sql</strong>: a query executed against the source, results upserted into the destination.
                <em>Initial snapshot</em> just flags that a snapshot job should be created; it does not start one. Saving validates the definition (columns exist, expressions parse); afterwards use the <i class="bi bi-check2-square"></i> <em>Validate / test</em> row action to dry-run it against real rows and <i class="bi bi-shield-check"></i> <em>Verify</em> to compare counts and samples.
            </help-hint>
            <div class="row g-3">
                <div class="col-md-2"><label class="form-label">Type</label><select v-model="editing.mapping_type" class="form-select" :disabled="editMode"><option value="table">table (CDC)</option><option value="sql">sql (query)</option></select></div>
                <div class="col-md-3"><label class="form-label">Mapping id</label><input v-model="editing.id" class="form-control mono" :disabled="editMode"></div>
                <div class="col-md-4"><label class="form-label">Name</label><input v-model="editing.name" class="form-control"></div>
                <div class="col-md-3 d-flex align-items-end gap-3">
                    <div class="form-check form-switch"><input v-model="editing.enabled" class="form-check-input" type="checkbox" id="m-en"><label class="form-check-label" for="m-en">Enabled</label></div>
                    <div class="form-check form-switch" v-if="editing.mapping_type === 'table'"><input v-model="editing.perform_initial_snapshot" class="form-check-input" type="checkbox" id="m-snap"><label class="form-check-label" for="m-snap">Initial snapshot</label></div>
                </div>
            </div>
            <hr>
            <div class="row g-3">
                <div class="col-md-6">
                    <h6 class="muted small text-uppercase">Source <span v-if="helper.loading" class="spinner-border spinner-border-sm ms-1"></span></h6>
                    <template v-if="editing.mapping_type === 'table'">
                        <div class="d-flex gap-2 mb-2">
                            <select v-model="helper.srcConn" class="form-select form-select-sm" @change="loadTables('src')"><option value="">browse with connection…</option><option v-for="c in store.connections" :key="c.id" :value="c.id">{{ c.id }} ({{ c.engine || 'mssql' }})</option></select>
                            <select v-if="helper.srcTables.length" class="form-select form-select-sm" @change="pickTable('src', $event.target.value)"><option value="">pick table…</option><option v-for="t in helper.srcTables" :key="t.key" :value="t.key">{{ t.key }} ({{ fmt.num(t.row_estimate) }})</option></select>
                        </div>
                        <div class="d-flex gap-2"><input v-model="editing.source_schema" class="form-control form-control-sm mono" placeholder="schema" @change="loadColumns('src')"><input v-model="editing.source_table" class="form-control form-control-sm mono" placeholder="table" @change="loadColumns('src')"></div>
                    </template>
                    <template v-else>
                        <textarea v-model="editing.source_query" class="form-control mono" rows="6" placeholder="SELECT … FROM … (executed on the active working set's source)"></textarea>
                    </template>
                </div>
                <div class="col-md-6">
                    <h6 class="muted small text-uppercase">Destination</h6>
                    <div class="d-flex gap-2 mb-2">
                        <select v-model="helper.dstConn" class="form-select form-select-sm" @change="loadTables('dst')"><option value="">browse with connection…</option><option v-for="c in sqlConnections" :key="c.id" :value="c.id">{{ c.id }} ({{ c.engine || 'mssql' }})</option></select>
                        <select v-if="helper.dstTables.length" class="form-select form-select-sm" @change="pickTable('dst', $event.target.value)"><option value="">pick table…</option><option v-for="t in helper.dstTables" :key="t.key" :value="t.key">{{ t.key }}</option></select>
                    </div>
                    <div class="d-flex gap-2"><input v-model="editing.destination_schema" class="form-control form-control-sm mono" placeholder="schema" @change="loadColumns('dst')"><input v-model="editing.destination_table" class="form-control form-control-sm mono" placeholder="table" @change="loadColumns('dst')"></div>
                </div>
            </div>

            <template v-if="editing.mapping_type === 'table'">
                <div class="d-flex justify-content-between align-items-center mt-3 mb-1">
                    <h6 class="muted small text-uppercase mb-0">Column mappings ({{ editing.column_mappings.length }})</h6>
                    <div class="btn-group btn-group-sm">
                        <button class="btn btn-outline-primary" :disabled="!sourceColumnNames.length && !destColumnNames.length" @click="autoMap()" title="match columns by name, pick PK as key"><i class="bi bi-magic"></i> Auto-map</button>
                        <button class="btn btn-outline-secondary" @click="addColumn()"><i class="bi bi-plus"></i> Column</button>
                    </div>
                </div>
                <div class="table-responsive">
                <table class="table table-sm column-grid mb-2">
                    <thead><tr><th>Source column</th><th>Destination column</th><th>Key</th><th>Transformation (DuckDB SQL)</th><th>Default</th><th>Ignore</th><th>Auto-generate</th><th></th></tr></thead>
                    <tbody>
                        <tr v-for="(c, i) in editing.column_mappings" :key="i">
                            <td><input v-model="c.source_column" class="form-control mono" list="srccols"></td>
                            <td><input v-model="c.destination_column" class="form-control mono" :class="{ 'is-invalid': !c.destination_column }" list="dstcols"></td>
                            <td class="text-center"><input type="checkbox" class="form-check-input" :checked="editing.key_columns.includes(c.destination_column)" :disabled="!c.destination_column" @change="toggleKey(c.destination_column)"></td>
                            <td><input v-model="c.transformation" class="form-control mono" :placeholder="'e.g. upper(&quot;' + (c.source_column || 'col') + '&quot;)'"></td>
                            <td><input v-model="c.default_value" class="form-control" placeholder="if NULL"></td>
                            <td class="text-center"><input v-model="c.ignore_changes" type="checkbox" class="form-check-input"></td>
                            <td><div class="d-flex gap-1"><select v-model="c.auto_generate" class="form-select"><option value="none">–</option><option value="on_insert">on insert</option><option value="on_init">on init</option></select><input v-if="c.auto_generate !== 'none'" v-model="c.auto_generate_expression" class="form-control mono" placeholder="uuid() / now()"></div></td>
                            <td><button class="btn btn-sm btn-link text-danger p-0" @click="removeColumn(i)"><i class="bi bi-x-lg"></i></button></td>
                        </tr>
                        <tr v-if="!editing.column_mappings.length"><td colspan="8" class="muted small">No columns – browse the source and destination above, then Auto-map.</td></tr>
                    </tbody>
                </table>
                </div>
                <datalist id="srccols"><option v-for="n in sourceColumnNames" :key="n" :value="n"></option></datalist>
                <datalist id="dstcols"><option v-for="n in destColumnNames" :key="n" :value="n"></option></datalist>
                <div class="row g-3 align-items-end">
                    <div class="col-md-4"><label class="form-label">Key columns (destination names) <span v-if="!editing.key_columns.length" class="text-warning">– none: destination PK will be used, otherwise a guess</span></label>
                        <input :value="editing.key_columns.join(', ')" @change="editing.key_columns = $event.target.value.split(',').map(s => s.trim()).filter(Boolean)" class="form-control form-control-sm mono"></div>
                    <div class="col-md-2"><label class="form-label">Sync mode</label><select v-model="editing.sync_mode" class="form-select form-select-sm"><option value="upsert">upsert (idempotent)</option><option value="incremental">incremental (insert only)</option><option value="full">full</option></select></div>
                    <div class="col-md-2"><label class="form-label">Batch size</label><input v-model.number="editing.batch_size" type="number" class="form-control form-control-sm"></div>
                    <div class="col-md-4 d-flex gap-3">
                        <div class="form-check"><input v-model="editing.sync_inserts" class="form-check-input" type="checkbox" id="s-i"><label class="form-check-label" for="s-i">inserts</label></div>
                        <div class="form-check"><input v-model="editing.sync_updates" class="form-check-input" type="checkbox" id="s-u"><label class="form-check-label" for="s-u">updates</label></div>
                        <div class="form-check"><input v-model="editing.sync_deletes" class="form-check-input" type="checkbox" id="s-d"><label class="form-check-label" for="s-d">deletes</label></div>
                    </div>
                </div>
            </template>
            <template v-else>
                <div class="row g-3 mt-1">
                    <div class="col-md-4"><label class="form-label">Key columns (for upsert)</label><input :value="editing.key_columns.join(', ')" @change="editing.key_columns = $event.target.value.split(',').map(s => s.trim()).filter(Boolean)" class="form-control form-control-sm mono"></div>
                    <div class="col-md-3"><label class="form-label">Sync mode</label><select v-model="editing.sync_mode" class="form-select form-select-sm"><option value="full">full (truncate + insert)</option><option value="incremental">incremental</option><option value="upsert">upsert</option></select></div>
                    <div class="col-md-2"><label class="form-label">Batch size</label><input v-model.number="editing.batch_size" type="number" class="form-control form-control-sm"></div>
                    <div class="col-md-3"><label class="form-label">Schedule (cron, optional)</label><input v-model="editing.sync_schedule" class="form-control form-control-sm mono"></div>
                </div>
            </template>
            <template #footer>
                <button class="btn btn-secondary" @click="editing = null">Cancel</button>
                <button class="btn btn-primary" @click="save()"><i class="bi bi-save"></i> Save</button>
            </template>
        </modal-dialog>
    </div>`,
};

import { api } from '../api.js';
import { store, fmt } from '../store.js';

export const Pipelines = {
    props: ['params'],
    data() {
        return {
            store, fmt, tab: 'pipelines', editing: null, health: {}, detail: null, busyId: null,
            worksets: [], opStatus: null, wsEditing: null, wsEditMode: false,
        };
    },
    computed: {
        items() {
            return store.pipelines.map(p => { const d = p.definition || {}; const r = p.runtime || null; return { id: d.id || (r && r.id), definition: d, runtime: r, state: r ? r.state : 'stopped' }; })
                .sort((a, b) => (a.id > b.id ? 1 : -1));
        },
        tableMappings() { return store.mappings.filter(m => m.mapping_type === 'table' && !m.is_multi_source); },
        activeWorkset() { return this.worksets.find(w => w.is_active); },
    },
    async created() {
        await this.loadWorksets();
        if (this.params?.id === 'new') this.openNew();
        else if (this.params?.id) this.openDetail(this.params.id);
        this.timer = setInterval(() => { if (this.detail) this.refreshDetail(); }, 4000);
    },
    unmounted() { clearInterval(this.timer); },
    methods: {
        openNew() { this.editing = { id: '', source_connection_id: '', destination_connection_id: '', mapping_ids: [], workset_id: null, auto_start: false, options: { initial_position: 'earliest', insert_mode: 'upsert', batch_size: 500, batch_wait_s: 0.5, expect_traffic: false } }; },
        openEdit(item) { const d = JSON.parse(JSON.stringify(item.definition)); d.options = { initial_position: 'earliest', insert_mode: 'upsert', batch_size: 500, batch_wait_s: 0.5, expect_traffic: false, ...(d.options || {}) }; this.editing = d; },
        async save() {
            const d = this.editing;
            if (!d.id || !d.source_connection_id || !d.destination_connection_id) { store.warn('id, source and destination are required'); return; }
            if (!d.mapping_ids.length && !d.workset_id) { store.warn('select at least one mapping'); return; }
            const r = await store.run(() => api.pipelines.define(d), { successMessage: 'Pipeline saved' });
            if (r) { this.editing = null; await store.loadRuntime(); }
        },
        async action(item, a) {
            if (a === 'stop' && !await store.ask(`Stop pipeline ${item.id}? Positions are committed; it resumes where it left off.`, { okLabel: 'Stop' })) return;
            this.busyId = item.id;
            try { await api.pipelines.action(item.id, a); store.success(`${item.id}: ${a}`); await store.loadRuntime(); if (this.detail && this.detail.id === item.id) await this.refreshDetail(); }
            catch (e) { store.error(e.message); } finally { this.busyId = null; }
        },
        async remove(item) {
            if (!await store.ask(`Delete pipeline definition ${item.id}? Committed positions are kept.`, { danger: true, okLabel: 'Delete' })) return;
            await store.run(() => api.pipelines.remove(item.id), { successMessage: 'deleted' }); await store.loadRuntime();
        },
        async resetPositions(item) {
            if (!await store.ask(`Reset committed positions of ${item.id}?\nThe next start re-reads from "${(item.definition.options || {}).initial_position || 'earliest'}". Combine with a new snapshot when the source retention was exceeded.`, { danger: true, okLabel: 'Reset' })) return;
            const r = await store.run(() => api.pipelines.resetPositions(item.id)); if (r) store.success(`removed ${r.removed.length} positions`);
        },
        async openDetail(id) { this.detail = { id }; await this.refreshDetail(); },
        async refreshDetail() {
            if (!this.detail) return;
            try {
                const [status, health] = await Promise.all([api.pipelines.status(this.detail.id), api.pipelines.health(this.detail.id).catch(() => null)]);
                this.detail = { id: this.detail.id, status, health };
            } catch (e) { /* stopped */ }
        },
        lagClass(s) { return s == null ? '' : s > 1800 ? 'text-danger fw-bold' : s > 300 ? 'text-warning fw-bold' : ''; },
        // ---- working sets (legacy operations API)
        async loadWorksets() { try { [this.worksets, this.opStatus] = await Promise.all([api.worksets.list(), api.operations.status()]); } catch (e) { /* ignore */ } },
        wsNew() { this.wsEditing = { id: '', name: '', description: '', source_connection_id: '', destination_connection_id: '', mappings: [], data_ingestion_check: false }; this.wsEditMode = false; },
        wsEdit(w) { this.wsEditing = { ...JSON.parse(JSON.stringify(w)), source_connection_id: w.source_connection_id || w.source_connection?.id || '', destination_connection_id: w.destination_connection_id || w.destination_connection?.id || '' }; this.wsEditMode = true; },
        async wsSave() {
            const w = this.wsEditing;
            const src = store.connectionById(w.source_connection_id), dst = store.connectionById(w.destination_connection_id);
            if (!w.id || !w.name || !src || !dst) { store.warn('id, name, source and destination connections are required'); return; }
            const body = { ...w, source_connection: null, destination_connection: null };
            const r = await store.run(() => this.wsEditMode ? api.worksets.update(body) : api.worksets.create(body), { successMessage: 'Working set saved' });
            if (r) { this.wsEditing = null; await this.loadWorksets(); }
        },
        async wsActivate(w) { await store.run(() => api.worksets.activate(w.id), { successMessage: `${w.id} activated` }); await this.loadWorksets(); },
        async wsRemove(w) { if (!await store.ask(`Delete working set ${w.id}?`, { danger: true, okLabel: 'Delete' })) return; await store.run(() => api.worksets.remove(w.id)); await this.loadWorksets(); },
        async opStart() { const r = await store.run(() => api.operations.start()); if (r) { store.success(r.message); (r.snapshot_results || []).forEach(s => (s.success ? store.success : store.warn).call(store, `${s.mapping_id}: ${s.message}`)); await Promise.all([this.loadWorksets(), store.loadRuntime()]); } },
        async opStop() { await store.run(() => api.operations.stop(), { successMessage: 'stopped' }); await Promise.all([this.loadWorksets(), store.loadRuntime()]); },
    },
    template: `<div>
        <ul class="nav tabs-clean mb-3">
            <li class="nav-item"><a class="nav-link" :class="{ active: tab === 'pipelines' }" href="#" @click.prevent="tab = 'pipelines'"><i class="bi bi-diagram-3"></i> Pipelines</a></li>
            <li class="nav-item"><a class="nav-link" :class="{ active: tab === 'worksets' }" href="#" @click.prevent="tab = 'worksets'"><i class="bi bi-collection"></i> Working sets (1.x compatibility)</a></li>
        </ul>

        <div v-if="tab === 'pipelines'">
            <help-hint id="pipelines" title="Step 3 - run the change stream">
                A pipeline binds one source connection, one destination and a set of table mappings. <strong>Start</strong> enables capture on the source (database + tables), opens the change stream and applies batches;
                positions are committed <em>after</em> the destination transaction, so a crash replays at most one batch and the applier is idempotent (upsert / delete-by-key).
                <strong>Pause</strong> keeps reading but stops applying (use it during destination maintenance), <strong>Stop</strong> releases the source. Per-table counters, lag and the last position are shown on each card; failures go to the dead-letter queue, not to the log.
                Working sets are the 1.x way of grouping mappings - kept for compatibility.
            </help-hint>
            <div class="toolbar">
                <button class="btn btn-primary btn-sm" @click="openNew()" data-tour="pipelines-new"><i class="bi bi-plus-lg"></i> Define pipeline</button>
                <span class="muted small ms-auto">{{ items.length }} defined · {{ store.runningPipelines().length }} running</span>
            </div>
            <empty-state v-if="!items.length" icon="bi-diagram-3" text="No pipelines defined yet."></empty-state>
            <div v-for="p in items" :key="p.id" class="card pipeline-card mb-3">
                <div class="card-header">
                    <div class="d-flex align-items-center gap-2">
                        <strong class="mono">{{ p.id }}</strong> <state-badge :state="p.state"></state-badge>
                        <span v-if="p.runtime" class="muted small">{{ p.runtime.engine }} → {{ p.runtime.destination }}</span>
                        <span v-else class="muted small">{{ p.definition.source_connection_id }} → {{ p.definition.destination_connection_id }}</span>
                        <span v-if="p.definition.auto_start" class="badge bg-secondary">auto-start</span>
                    </div>
                    <div class="btn-group btn-group-sm">
                        <button class="btn btn-outline-success" :disabled="busyId === p.id || ['running','paused'].includes(p.state)" @click="action(p, 'start')" title="Start"><i class="bi bi-play-fill"></i></button>
                        <button class="btn btn-outline-warning" :disabled="p.state !== 'running'" @click="action(p, 'pause')" title="Pause (events are held, positions not advanced)"><i class="bi bi-pause-fill"></i></button>
                        <button class="btn btn-outline-info" :disabled="p.state !== 'paused'" @click="action(p, 'resume')" title="Resume"><i class="bi bi-skip-forward-fill"></i></button>
                        <button class="btn btn-outline-danger" :disabled="!['running','paused','error','starting'].includes(p.state)" @click="action(p, 'stop')" title="Stop"><i class="bi bi-stop-fill"></i></button>
                        <button class="btn btn-outline-secondary" @click="openDetail(p.id)" title="Details & health"><i class="bi bi-info-circle"></i></button>
                        <button class="btn btn-outline-secondary" :disabled="!p.definition.id" @click="openEdit(p)" title="Edit definition"><i class="bi bi-pencil"></i></button>
                        <button class="btn btn-outline-secondary" :disabled="['running','paused'].includes(p.state)" @click="resetPositions(p)" title="Reset positions"><i class="bi bi-arrow-counterclockwise"></i></button>
                        <button class="btn btn-outline-danger" :disabled="['running','paused'].includes(p.state)" @click="remove(p)" title="Delete"><i class="bi bi-trash"></i></button>
                    </div>
                </div>
                <div class="card-body" v-if="p.runtime">
                    <div class="row g-2 mb-2">
                        <div class="col-6 col-md-2"><div class="stat"><div class="v">{{ fmt.num(p.runtime.events_read) }}</div><div class="l">read</div></div></div>
                        <div class="col-6 col-md-2"><div class="stat"><div class="v text-success">{{ fmt.num(p.runtime.events_applied) }}</div><div class="l">applied</div></div></div>
                        <div class="col-6 col-md-2"><div class="stat"><div class="v" :class="{ 'text-danger': p.runtime.events_failed }">{{ fmt.num(p.runtime.events_failed) }}</div><div class="l">failed → DLQ</div></div></div>
                        <div class="col-6 col-md-2"><div class="stat"><div class="v">{{ fmt.num(p.runtime.queue_depth) }}</div><div class="l">queue</div></div></div>
                        <div class="col-6 col-md-2"><div class="stat"><div class="v" :class="lagClass(p.runtime.lag_seconds)">{{ p.runtime.lag_seconds == null ? '–' : fmt.secs(p.runtime.lag_seconds) }}</div><div class="l">lag</div></div></div>
                        <div class="col-6 col-md-2"><div class="stat"><div class="v">{{ fmt.secs(p.runtime.uptime_seconds) }}</div><div class="l">uptime · {{ p.runtime.batches }} batches</div></div></div>
                    </div>
                    <div class="small muted">tables: <span class="mono">{{ p.runtime.tables.join(', ') }}</span> · last event {{ fmt.ago(p.runtime.last_event_at) }}</div>
                    <div v-if="p.runtime.unsafe_key_tables.length" class="small text-warning"><i class="bi bi-exclamation-triangle"></i> guessed key columns for {{ p.runtime.unsafe_key_tables.join(', ') }} – set key_columns in the mapping</div>
                    <div v-if="p.runtime.last_error" class="small text-danger mono mt-1">last error: {{ p.runtime.last_error }}</div>
                </div>
                <div class="card-body muted small" v-else>mappings: {{ (p.definition.mapping_ids || []).join(', ') || ('working set ' + p.definition.workset_id) }} · options: {{ JSON.stringify(p.definition.options || {}) }}</div>
            </div>
        </div>

        <div v-if="tab === 'worksets'">
            <div class="alert alert-light border small">Working sets are the 1.x concept (source + destination + mappings, one active at a time). <strong>Start</strong> builds a pipeline <code>ws_&lt;id&gt;</code> and creates background snapshot jobs for mappings with "initial snapshot". New deployments should define pipelines directly.</div>
            <div class="toolbar">
                <button class="btn btn-primary btn-sm" @click="wsNew()"><i class="bi bi-plus-lg"></i> New working set</button>
                <template v-if="activeWorkset">
                    <span class="ms-2 small">active: <strong>{{ activeWorkset.name }}</strong> <state-badge :state="opStatus ? opStatus.status : 'stopped'"></state-badge></span>
                    <button class="btn btn-success btn-sm" @click="opStart()" :disabled="opStatus && ['running','paused'].includes(opStatus.status)"><i class="bi bi-play-fill"></i> Start sync</button>
                    <button class="btn btn-danger btn-sm" @click="opStop()" :disabled="!opStatus || !['running','paused'].includes(opStatus.status)"><i class="bi bi-stop-fill"></i> Stop</button>
                </template>
            </div>
            <div class="card"><div class="card-body p-0">
                <table class="table mb-0"><thead><tr><th>Working set</th><th>Source</th><th>Destination</th><th>Mappings</th><th>Active</th><th></th></tr></thead>
                <tbody>
                    <tr v-for="w in worksets" :key="w.id">
                        <td><strong>{{ w.name }}</strong><div class="small muted mono">{{ w.id }}</div></td>
                        <td class="mono small">{{ w.source_connection.server }}/{{ w.source_connection.database }} <span class="badge bg-dark">{{ w.source_connection.engine || 'mssql' }}</span></td>
                        <td class="mono small">{{ w.destination_connection.server }}/{{ w.destination_connection.database }} <span class="badge bg-dark">{{ w.destination_connection.engine || 'mssql' }}</span></td>
                        <td class="small">{{ (w.mappings || []).join(', ') }}</td>
                        <td><i class="bi" :class="w.is_active ? 'bi-check-circle-fill text-success' : 'bi-circle muted'"></i></td>
                        <td class="text-end"><div class="btn-group btn-group-sm">
                            <button class="btn btn-outline-primary" :disabled="w.is_active" @click="wsActivate(w)">activate</button>
                            <button class="btn btn-outline-secondary" @click="wsEdit(w)"><i class="bi bi-pencil"></i></button>
                            <button class="btn btn-outline-danger" @click="wsRemove(w)"><i class="bi bi-trash"></i></button></div></td>
                    </tr>
                    <tr v-if="!worksets.length"><td colspan="6"><div class="empty"><i class="bi bi-collection"></i>No working sets.</div></td></tr>
                </tbody></table>
            </div></div>
        </div>

        <!-- pipeline definition editor -->
        <modal-dialog v-if="editing" :title="editing.id ? 'Pipeline ' + editing.id : 'Define pipeline'" size="lg" @close="editing = null">
            <help-hint id="pipeline-editor" icon="bi-info-circle">
                One pipeline per source connection; select the table mappings it should drive (only mappings whose source matches the connection make sense). <em>Initial position</em> <code>latest</code> starts from "now" (pair it with a snapshot), <code>earliest</code> replays whatever the source still retains.
                Batch size / wait bound the apply transaction size and the latency; insert mode <code>upsert</code> is the safe default for replays. Saving does not start the pipeline.
            </help-hint>
            <div class="row g-3">
                <div class="col-md-4"><label class="form-label">Pipeline id</label><input v-model="editing.id" class="form-control mono" placeholder="customers"></div>
                <div class="col-md-4"><label class="form-label">Source connection</label><select v-model="editing.source_connection_id" class="form-select"><option value="">…</option><option v-for="c in store.connections" :key="c.id" :value="c.id">{{ c.id }} ({{ c.engine || 'mssql' }})</option></select></div>
                <div class="col-md-4"><label class="form-label">Destination connection</label><select v-model="editing.destination_connection_id" class="form-select"><option value="">…</option><option v-for="c in store.connections" :key="c.id" :value="c.id">{{ c.id }} ({{ c.engine || 'mssql' }})</option></select></div>
                <div class="col-md-12"><label class="form-label">Table mappings</label>
                    <div class="border rounded p-2" style="max-height: 220px; overflow:auto">
                        <div v-for="m in tableMappings" :key="m.id" class="form-check"><input class="form-check-input" type="checkbox" :id="'pm-' + m.id" :value="m.id" v-model="editing.mapping_ids"><label class="form-check-label" :for="'pm-' + m.id"><span class="mono">{{ m.id }}</span> <span class="muted small">{{ m.source_schema }}.{{ m.source_table }} → {{ m.destination_schema }}.{{ m.destination_table }}</span></label></div>
                        <div v-if="!tableMappings.length" class="muted small">No table mappings – create one first.</div>
                    </div></div>
                <div class="col-md-3"><label class="form-label">Initial position</label><select v-model="editing.options.initial_position" class="form-select"><option value="earliest">earliest retained change</option><option value="latest">now (skip history)</option></select></div>
                <div class="col-md-3"><label class="form-label">Insert mode</label><select v-model="editing.options.insert_mode" class="form-select"><option value="upsert">upsert (idempotent)</option><option value="insert">plain insert</option></select></div>
                <div class="col-md-2"><label class="form-label">Batch size</label><input v-model.number="editing.options.batch_size" type="number" class="form-control"></div>
                <div class="col-md-2"><label class="form-label">Batch wait (s)</label><input v-model.number="editing.options.batch_wait_s" type="number" step="0.1" class="form-control"></div>
                <div class="col-md-2 d-flex flex-column justify-content-end gap-1">
                    <div class="form-check"><input v-model="editing.auto_start" class="form-check-input" type="checkbox" id="p-auto"><label class="form-check-label" for="p-auto">auto-start</label></div>
                    <div class="form-check"><input v-model="editing.options.expect_traffic" class="form-check-input" type="checkbox" id="p-tr"><label class="form-check-label" for="p-tr">alert if silent</label></div>
                </div>
            </div>
            <template #footer><button class="btn btn-secondary" @click="editing = null">Cancel</button><button class="btn btn-primary" @click="save()">Save</button></template>
        </modal-dialog>

        <!-- detail -->
        <modal-dialog v-if="detail" :title="'Pipeline ' + detail.id" size="lg" @close="detail = null">
            <div v-if="!detail.status" class="muted">loading…</div>
            <template v-else>
                <div class="row g-3">
                    <div class="col-md-6">
                        <h6 class="muted small text-uppercase">Status</h6>
                        <dl class="kv">
                            <dt>state</dt><dd><state-badge :state="detail.status.state"></state-badge></dd>
                            <dt>started</dt><dd>{{ fmt.ts(detail.status.started_at) }}</dd>
                            <dt>last event</dt><dd>{{ fmt.ts(detail.status.last_event_at) }}</dd>
                            <dt>lag</dt><dd>{{ detail.status.lag_seconds == null ? '–' : fmt.secs(detail.status.lag_seconds) }}</dd>
                            <dt>queue</dt><dd>{{ fmt.num(detail.status.queue_depth) }}</dd>
                            <dt>last error</dt><dd class="mono text-danger">{{ detail.status.last_error || '–' }}</dd>
                        </dl>
                        <h6 class="muted small text-uppercase mt-3">Committed positions</h6>
                        <dl class="kv"><template v-for="(p, k) in detail.status.positions" :key="k"><dt class="mono">{{ k }}</dt><dd class="mono">{{ fmt.pos(p) }}</dd></template></dl>
                    </div>
                    <div class="col-md-6">
                        <h6 class="muted small text-uppercase">Health facts (rule engine input)</h6>
                        <facts-tree v-if="detail.health" :value="detail.health"></facts-tree>
                        <div v-else class="muted small">not running</div>
                    </div>
                </div>
            </template>
        </modal-dialog>

        <!-- workset editor -->
        <modal-dialog v-if="wsEditing" :title="wsEditMode ? 'Edit working set' : 'New working set'" size="lg" @close="wsEditing = null">
            <div class="row g-3">
                <div class="col-md-4"><label class="form-label">Id</label><input v-model="wsEditing.id" class="form-control mono" :disabled="wsEditMode"></div>
                <div class="col-md-8"><label class="form-label">Name</label><input v-model="wsEditing.name" class="form-control"></div>
                <div class="col-md-6"><label class="form-label">Source connection</label><select v-model="wsEditing.source_connection_id" class="form-select"><option value="">…</option><option v-for="c in store.connections" :key="c.id" :value="c.id">{{ c.id }}</option></select></div>
                <div class="col-md-6"><label class="form-label">Destination connection</label><select v-model="wsEditing.destination_connection_id" class="form-select"><option value="">…</option><option v-for="c in store.connections" :key="c.id" :value="c.id">{{ c.id }}</option></select></div>
                <div class="col-12"><label class="form-label">Mappings</label>
                    <div class="border rounded p-2" style="max-height: 200px; overflow:auto">
                        <div v-for="m in store.mappings" :key="m.id" class="form-check"><input class="form-check-input" type="checkbox" :id="'wm-' + m.id" :value="m.id" v-model="wsEditing.mappings"><label class="form-check-label" :for="'wm-' + m.id"><span class="mono">{{ m.id }}</span> <span class="badge bg-secondary">{{ m.mapping_type }}</span></label></div>
                    </div></div>
            </div>
            <template #footer><button class="btn btn-secondary" @click="wsEditing = null">Cancel</button><button class="btn btn-primary" @click="wsSave()">Save</button></template>
        </modal-dialog>
    </div>`,
};

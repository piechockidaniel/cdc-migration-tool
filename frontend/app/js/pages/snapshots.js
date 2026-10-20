import { api } from '../api.js';
import { store, fmt } from '../store.js';

const DAYS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];

export const Snapshots = {
    props: ['params', 'query'],
    data() {
        return { store, fmt, DAYS, creating: null, filter: '', detail: null, busyId: null };
    },
    computed: {
        jobs() { return store.snapshots.filter(j => !this.filter || j.state === this.filter); },
        tableMappings() { return store.mappings.filter(m => m.mapping_type === 'table' && !m.is_multi_source); },
        pipelineIds() { return store.pipelines.map(p => (p.definition && p.definition.id) || (p.runtime && p.runtime.id)).filter(Boolean); },
        counts() { const c = {}; for (const j of store.snapshots) c[j.state] = (c[j.state] || 0) + 1; return c; },
    },
    created() { if (this.params?.id === 'new') this.openNew(this.query?.mapping); },
    methods: {
        openNew(mappingId) {
            this.creating = { mapping_id: mappingId || '', source_connection_id: '', destination_connection_id: '', pipeline_id: '', key_columns: '', where: '',
                              chunk_size: 5000, min_chunk_size: 500, max_chunk_size: 50000, max_chunk_seconds: 20, max_rows_per_second: null, pause_between_chunks_ms: 0, max_retries: 5,
                              useWindow: false, winStart: '22:00', winEnd: '06:00', winDays: [0, 1, 2, 3, 4], start: true };
            // prefill connections from a pipeline that uses the mapping
            const p = store.pipelines.find(p => p.definition && (p.definition.mapping_ids || []).includes(mappingId));
            if (p) { this.creating.source_connection_id = p.definition.source_connection_id; this.creating.destination_connection_id = p.definition.destination_connection_id; this.creating.pipeline_id = p.definition.id; }
        },
        onPipelineChange() { const p = store.pipelines.find(p => p.definition && p.definition.id === this.creating.pipeline_id); if (p) { this.creating.source_connection_id = p.definition.source_connection_id; this.creating.destination_connection_id = p.definition.destination_connection_id; } },
        async create() {
            const c = this.creating;
            if (!c.mapping_id || !c.source_connection_id || !c.destination_connection_id) { store.warn('mapping, source and destination are required'); return; }
            const body = { ...c, pipeline_id: c.pipeline_id || null, key_columns: c.key_columns ? c.key_columns.split(',').map(s => s.trim()).filter(Boolean) : null,
                           where: c.where || null, max_rows_per_second: c.max_rows_per_second || null,
                           windows: c.useWindow ? [{ start: c.winStart, end: c.winEnd, days: c.winDays }] : [] };
            for (const k of ['useWindow', 'winStart', 'winEnd', 'winDays']) delete body[k];
            const r = await store.run(() => api.snapshots.create(body), { successMessage: 'Snapshot job created' });
            if (r) { this.creating = null; await store.loadRuntime(); }
        },
        async action(j, a) {
            if (a === 'cancel' && !await store.ask(`Cancel snapshot ${j.id}? Progress is kept; it can be reset or resumed later.`, { danger: true, okLabel: 'Cancel job' })) return;
            if (a === 'reset' && !await store.ask(`Reset ${j.id} to the beginning? Rows already copied stay in the destination (upserts).`, { okLabel: 'Reset' })) return;
            this.busyId = j.id;
            try { await api.snapshots.action(j.id, a); await store.loadRuntime(); } catch (e) { store.error(e.message); } finally { this.busyId = null; }
        },
        async remove(j) { if (!await store.ask(`Delete job ${j.id}?`, { danger: true, okLabel: 'Delete' })) return; await store.run(() => api.snapshots.remove(j.id)); await store.loadRuntime(); },
        pct(j) { return j.progress == null ? null : Math.round(j.progress * 100); },
        eta(j) { if (!j.rows_per_second || !j.estimated_rows || j.state !== 'running') return null; const left = Math.max(j.estimated_rows - j.rows_copied, 0); return fmt.secs(left / j.rows_per_second); },
    },
    template: `<div>
        <help-hint id="snapshots" title="Step 4 - initial load without hurting the source" tone="warning">
            A snapshot copies the existing rows of a table in <strong>keyset chunks</strong> (<code>WHERE key &gt; last_key ORDER BY key LIMIT n</code>) - never a full scan, never a long transaction, never a 16 TB SELECT.
            Each chunk is upserted and its last key persisted, so a job can be paused, throttled (rows/s), limited to a maintenance window, or resumed after a restart. Order of operations: <strong>start the pipeline first</strong>, then the snapshot - changes made during the copy are captured by CDC and win by key.
            Use <em>Verify</em> afterwards to compare row counts / checksums between source and destination.
        </help-hint>
        <div class="toolbar">
            <button class="btn btn-primary btn-sm" @click="openNew()" data-tour="snapshots-new"><i class="bi bi-plus-lg"></i> New snapshot job</button>
            <select v-model="filter" class="form-select form-select-sm"><option value="">all states</option><option v-for="s in ['running','paused','pending','completed','failed','cancelled']" :key="s" :value="s">{{ s }} ({{ counts[s] || 0 }})</option></select>
            <span class="muted small ms-auto">{{ jobs.length }} jobs</span>
        </div>
        <empty-state v-if="!jobs.length" icon="bi-layers" text="No snapshot jobs."></empty-state>
        <div v-for="j in jobs" :key="j.id" class="card mb-3">
            <div class="card-header">
                <div class="d-flex align-items-center gap-2 flex-wrap">
                    <strong class="mono">{{ j.id }}</strong> <state-badge :state="j.state"></state-badge>
                    <span v-if="j.waiting_for_window" class="badge bg-secondary">waiting for window</span>
                    <span class="muted small mono">{{ j.source_schema }}.{{ j.source_table }} → {{ j.dest_schema }}.{{ j.dest_table }} · key {{ j.key_columns.join(', ') }}</span>
                    <span v-if="j.pipeline_id" class="badge bg-light text-dark border">→ pipeline {{ j.pipeline_id }}</span>
                </div>
                <div class="btn-group btn-group-sm">
                    <button class="btn btn-outline-success" :disabled="busyId === j.id || ['running','completed'].includes(j.state)" @click="action(j, 'start')" title="Start"><i class="bi bi-play-fill"></i></button>
                    <button class="btn btn-outline-warning" :disabled="j.state !== 'running'" @click="action(j, 'pause')" title="Pause"><i class="bi bi-pause-fill"></i></button>
                    <button class="btn btn-outline-info" :disabled="j.state !== 'paused'" @click="action(j, 'resume')" title="Resume"><i class="bi bi-skip-forward-fill"></i></button>
                    <button class="btn btn-outline-danger" :disabled="!['running','paused','pending'].includes(j.state)" @click="action(j, 'cancel')" title="Cancel"><i class="bi bi-x-lg"></i></button>
                    <button class="btn btn-outline-secondary" :disabled="j.state === 'running'" @click="action(j, 'reset')" title="Reset to beginning"><i class="bi bi-arrow-counterclockwise"></i></button>
                    <button class="btn btn-outline-secondary" @click="detail = j" title="Details"><i class="bi bi-info-circle"></i></button>
                    <button class="btn btn-outline-danger" :disabled="j.state === 'running'" @click="remove(j)" title="Delete"><i class="bi bi-trash"></i></button>
                </div>
            </div>
            <div class="card-body">
                <div class="d-flex align-items-center gap-3 mb-2">
                    <div class="progress flex-grow-1"><div class="progress-bar" :class="{ 'bg-danger': j.state === 'failed', 'bg-success': j.state === 'completed', 'progress-bar-striped progress-bar-animated': j.state === 'running' }" :style="{ width: (pct(j) ?? 0) + '%' }"></div></div>
                    <strong style="width: 48px; text-align: right">{{ pct(j) == null ? '?' : pct(j) + '%' }}</strong>
                </div>
                <div class="row g-2 small">
                    <div class="col-6 col-md-2">rows <strong>{{ fmt.num(j.rows_copied) }}</strong> <span class="muted">/ {{ fmt.num(j.estimated_rows) }}</span></div>
                    <div class="col-6 col-md-2">chunks <strong>{{ fmt.num(j.chunks_done) }}</strong> <span class="muted">(size {{ fmt.num(j.chunk_size) }})</span></div>
                    <div class="col-6 col-md-2">rate <strong>{{ fmt.num(j.rows_per_second) }}</strong> <span class="muted">rows/s</span></div>
                    <div class="col-6 col-md-2">avg chunk <strong>{{ fmt.num(j.avg_chunk_seconds, 2) }} s</strong></div>
                    <div class="col-6 col-md-2">errors <strong :class="{ 'text-danger': j.chunk_errors }">{{ j.chunk_errors }}</strong> <span v-if="j.rows_failed" class="text-warning">· {{ fmt.num(j.rows_failed) }} rows → DLQ</span></div>
                    <div class="col-6 col-md-2">ETA <strong>{{ eta(j) || '–' }}</strong></div>
                </div>
                <div class="small muted mt-1 mono">last key {{ JSON.stringify(j.last_key) }} <span v-if="j.start_position">· source position at start {{ fmt.pos(j.start_position) }}</span></div>
                <div v-if="j.error" class="small text-danger mono mt-1">{{ j.error }}</div>
            </div>
        </div>

        <modal-dialog v-if="detail" :title="'Snapshot job ' + detail.id" size="lg" @close="detail = null"><json-view :value="detail" max-height="560px"></json-view></modal-dialog>

        <modal-dialog v-if="creating" title="New snapshot job" size="lg" @close="creating = null">
            <help-hint id="snapshot-editor" icon="bi-info-circle" tone="warning">
                Pick the mapping and (recommended) the running pipeline: the job records the pipeline's current position so you can prove that no change between "snapshot started" and "CDC took over" was missed.
                Chunk key = the primary key or any unique, indexed, monotonic column set; use a <em>range filter</em> to split a huge table into several jobs that run in parallel or on different days. Start small (chunk 5-10k rows, a few thousand rows/s) and raise the throttle while watching the source's load.
            </help-hint>
            <div class="row g-3">
                <div class="col-md-6"><label class="form-label">Mapping</label><select v-model="creating.mapping_id" class="form-select"><option value="">…</option><option v-for="m in tableMappings" :key="m.id" :value="m.id">{{ m.id }} – {{ m.source_schema }}.{{ m.source_table }}</option></select></div>
                <div class="col-md-6"><label class="form-label">Pipeline (hands over the start position)</label><select v-model="creating.pipeline_id" class="form-select" @change="onPipelineChange"><option value="">none</option><option v-for="id in pipelineIds" :key="id" :value="id">{{ id }}</option></select></div>
                <div class="col-md-6"><label class="form-label">Source connection (read replica recommended)</label><select v-model="creating.source_connection_id" class="form-select"><option value="">…</option><option v-for="c in store.connections" :key="c.id" :value="c.id">{{ c.id }}</option></select></div>
                <div class="col-md-6"><label class="form-label">Destination connection</label><select v-model="creating.destination_connection_id" class="form-select"><option value="">…</option><option v-for="c in store.connections" :key="c.id" :value="c.id">{{ c.id }}</option></select></div>
                <div class="col-md-6"><label class="form-label">Chunk key columns (source names; default = PK)</label><input v-model="creating.key_columns" class="form-control mono" placeholder="CustomerID"></div>
                <div class="col-md-6"><label class="form-label">Range filter (WHERE, for splitting huge tables into several jobs)</label><input v-model="creating.where" class="form-control mono" placeholder="CustomerID BETWEEN 1 AND 50000000"></div>
                <div class="col-md-3"><label class="form-label">Chunk size</label><input v-model.number="creating.chunk_size" type="number" class="form-control"></div>
                <div class="col-md-3"><label class="form-label">Min / max chunk</label><div class="d-flex gap-1"><input v-model.number="creating.min_chunk_size" type="number" class="form-control"><input v-model.number="creating.max_chunk_size" type="number" class="form-control"></div></div>
                <div class="col-md-3"><label class="form-label">Max chunk seconds (adaptive)</label><input v-model.number="creating.max_chunk_seconds" type="number" step="0.5" class="form-control"></div>
                <div class="col-md-3"><label class="form-label">Rows / second cap</label><input v-model.number="creating.max_rows_per_second" type="number" class="form-control" placeholder="unlimited"></div>
                <div class="col-md-3"><label class="form-label">Pause between chunks (ms)</label><input v-model.number="creating.pause_between_chunks_ms" type="number" class="form-control"></div>
                <div class="col-md-3"><label class="form-label">Max retries per chunk</label><input v-model.number="creating.max_retries" type="number" class="form-control"></div>
                <div class="col-md-6 d-flex align-items-end gap-3">
                    <div class="form-check"><input v-model="creating.useWindow" class="form-check-input" type="checkbox" id="sn-w"><label class="form-check-label" for="sn-w">Only inside a maintenance window</label></div>
                    <div class="form-check"><input v-model="creating.start" class="form-check-input" type="checkbox" id="sn-s"><label class="form-check-label" for="sn-s">Start immediately</label></div>
                </div>
                <div class="col-12" v-if="creating.useWindow">
                    <div class="d-flex align-items-center gap-2 flex-wrap">
                        <input v-model="creating.winStart" type="time" class="form-control form-control-sm w-auto"> to <input v-model="creating.winEnd" type="time" class="form-control form-control-sm w-auto">
                        <div class="form-check form-check-inline" v-for="(d, i) in DAYS" :key="d"><input class="form-check-input" type="checkbox" :id="'d' + i" :value="i" v-model="creating.winDays"><label class="form-check-label" :for="'d' + i">{{ d }}</label></div>
                    </div>
                </div>
            </div>
            <template #footer><button class="btn btn-secondary" @click="creating = null">Cancel</button><button class="btn btn-primary" @click="create()"><i class="bi bi-layers"></i> Create</button></template>
        </modal-dialog>
    </div>`,
};

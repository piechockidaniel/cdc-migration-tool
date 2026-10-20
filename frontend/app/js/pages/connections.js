import { api } from '../api.js';
import { store, fmt } from '../store.js';

export const ENGINES = {
    mssql: { label: 'SQL Server (CDC)', port: 1433, windowsAuth: true, fields: [
        { key: 'driver', label: 'ODBC driver', type: 'select', top: true, options: ['ODBC Driver 18 for SQL Server', 'ODBC Driver 17 for SQL Server', 'SQL Server'] },
        { key: 'read_only_intent', label: 'ApplicationIntent=ReadOnly (AG secondary, snapshots)', type: 'bool', top: true },
        { key: 'service_broker_queue', label: 'Service Broker queue (push mode)', type: 'text', hint: 'e.g. dbo.cdcsync_notify_queue – see script below' },
        { key: 'row_filter', label: 'Row filter', type: 'select', options: ['all', 'all update old'], hint: "'all update old' delivers before-images of updates" },
        { key: 'poll_min_seconds', label: 'Poll min (s)', type: 'number', hint: 'fastest watermark cadence while busy (default 0.25)' },
        { key: 'poll_max_seconds', label: 'Poll max (s)', type: 'number', hint: 'back-off ceiling while idle (default 30)' },
        { key: 'capture_instances', label: 'Capture instance overrides', type: 'json', hint: '{"dbo.Customers": "dbo_Customers_v2"}' },
    ] },
    postgresql: { label: 'PostgreSQL (logical slot)', port: 5432, fields: [
        { key: 'slot_name', label: 'Replication slot', type: 'text', hint: 'default cdcsync_<connection id>' },
        { key: 'plugin', label: 'Output plugin', type: 'select', options: ['wal2json', 'test_decoding'] },
        { key: 'notify_channel', label: 'LISTEN channel (optional wake-up)', type: 'text' },
        { key: 'sslmode', label: 'sslmode', type: 'select', options: ['', 'require', 'verify-ca', 'verify-full', 'prefer', 'disable'] },
    ] },
    mariadb: { label: 'MariaDB / MySQL (binlog)', port: 3306, fields: [
        { key: 'server_id', label: 'Replica server_id', type: 'number', hint: 'must be unique in the replication topology (default 4379)' },
    ] },
    mongodb: { label: 'MongoDB (change streams)', port: 27017, fields: [
        { key: 'uri', label: 'Connection URI (overrides host/port)', type: 'text', hint: 'mongodb://user:pass@host1,host2/?replicaSet=rs0' },
        { key: 'auth_database', label: 'Auth database', type: 'text' },
        { key: 'options', label: 'Connection options', type: 'text', hint: 'replicaSet=rs0&readPreference=secondaryPreferred' },
        { key: 'full_document_before_change', label: 'Before images', type: 'select', options: ['whenAvailable', 'required', 'off'] },
    ] },
    oracle: { label: 'Oracle (LogMiner)', port: 1521, fields: [
        { key: 'service_name', label: 'Service name', type: 'text', hint: 'e.g. FREEPDB1 (or give a full dsn)' },
        { key: 'dsn', label: 'DSN (optional)', type: 'text' },
        { key: 'schema', label: 'Schema / owner', type: 'text', hint: 'defaults to the user name' },
        { key: 'supplemental_mode', label: 'Supplemental logging', type: 'select', options: ['ALL', 'PRIMARY KEY'] },
        { key: 'mode', label: 'Connect mode', type: 'select', options: ['', 'SYSDBA'] },
    ] },
    sqlite: { label: 'SQLite (destination only)', fields: [{ key: 'path', label: 'File path', type: 'text' }] },
    demo: { label: 'Demo (synthetic source, no database)', fields: [
        { key: 'rate_per_second', label: 'Changes per second', type: 'number', hint: 'default 5' },
        { key: 'initial_rows', label: 'Initial rows', type: 'number', hint: 'default 5000' },
        { key: 'poison_ratio', label: 'Poison ratio', type: 'number', hint: '0.05 = 5% of inserts with NULL Name (exercise the DLQ)' },
    ] },
};

function emptyConnection() {
    return { id: '', name: '', engine: 'mssql', server: '', port: null, database: '', use_windows_auth: false, username: '', password: '',
             driver: 'ODBC Driver 18 for SQL Server', login_timeout: 15, read_only_intent: false, options: {} };
}

export const Connections = {
    props: ['params'],
    data() {
        return {
            store, fmt, ENGINES,
            editing: null, editMode: false, optionsJsonText: {}, testResult: null, busy: false,
            prereq: { open: false, conn: null, tables: '', result: null, busy: false },
            browser: { open: false, conn: null, tables: [], columns: {}, selected: null, busy: false, filter: '', status: null },
            broker: null,
        };
    },
    computed: {
        rows() { return store.connections.map(c => ({ ...c, engine: c.engine || 'mssql', target: `${c.server || ''}${c.port ? ':' + c.port : ''} / ${c.database || ''}` })); },
        columns() { return [
            { key: 'id', label: 'Connection', sortable: true }, { key: 'engine', label: 'Engine', sortable: true },
            { key: 'target', label: 'Server / database' }, { key: 'auth', label: 'Auth' }, { key: 'actions', label: '', class: 'text-end' },
        ]; },
        engineDef() { return ENGINES[this.editing?.engine] || ENGINES.mssql; },
    },
    created() { if (this.params?.id === 'new') this.openNew(); },
    methods: {
        openNew() { this.editing = emptyConnection(); this.editMode = false; this.optionsJsonText = {}; this.testResult = null; },
        async openEdit(c) {
            const full = await store.run(() => api.connections.get(c.id));
            if (!full) return;
            this.editing = { ...emptyConnection(), ...full, options: { ...(full.options || {}) } };
            this.optionsJsonText = {};
            for (const f of (ENGINES[this.editing.engine] || {}).fields || []) if (f.type === 'json' && this.editing.options[f.key]) this.optionsJsonText[f.key] = JSON.stringify(this.editing.options[f.key]);
            this.editMode = true; this.testResult = null;
        },
        onEngineChange() { const d = ENGINES[this.editing.engine]; if (d && d.port && !this.editing.port) this.editing.port = d.port; if (!d.windowsAuth) this.editing.use_windows_auth = false; },
        payload() {
            const c = JSON.parse(JSON.stringify(this.editing));
            c.id = (c.id || c.name || '').trim().toLowerCase().replace(/\s+/g, '_').replace(/[^a-z0-9_.-]/g, '') || null;
            if (!c.port) delete c.port;
            const opts = {};
            for (const f of this.engineDef.fields || []) {
                if (f.top) continue;
                let v = c.options?.[f.key];
                if (f.type === 'json') { const t = (this.optionsJsonText[f.key] || '').trim(); if (t) { try { v = JSON.parse(t); } catch { throw new Error(`${f.label}: invalid JSON`); } } else { v = undefined; } }
                if (f.type === 'number' && v !== undefined && v !== null && v !== '') v = Number(v);
                if (v !== undefined && v !== null && v !== '') opts[f.key] = v;
            }
            c.options = opts;
            return c;
        },
        async save() {
            let c; try { c = this.payload(); } catch (e) { store.error(e.message); return; }
            const needsServer = !['sqlite', 'demo'].includes(c.engine) && !(c.options && c.options.uri);
            if (!c.name || (needsServer && !c.server)) { store.warn('Name and server are required'); return; }
            const r = await store.run(() => this.editMode ? api.connections.update(c) : api.connections.save(c), { successMessage: 'Connection saved' });
            if (r) { this.editing = null; await store.loadReference(); }
        },
        async test() {
            let c; try { c = this.payload(); } catch (e) { store.error(e.message); return; }
            this.busy = true; this.testResult = null;
            try { this.testResult = await api.connectors.test({ connection: { ...c, ...c.options } }); }
            catch (e) { this.testResult = { success: false, message: e.message }; }
            finally { this.busy = false; }
        },
        async testById(c) { const r = await store.run(() => api.connectors.test({ connection_id: c.id })); if (r) r.success ? store.success(`${c.id}: ${r.message}`) : store.error(`${c.id}: ${r.message}`); },
        async remove(c) {
            if (!await store.ask(`Delete connection "${c.id}"? Mappings/pipelines referencing it will fail.`, { danger: true, okLabel: 'Delete' })) return;
            await store.run(() => api.connections.remove(c.id), { successMessage: 'Connection deleted' });
            await store.loadReference();
        },
        // ---- prerequisites
        openPrereq(c) { this.prereq = { open: true, conn: c, tables: '', result: null, busy: false }; },
        prereqBody() { return { connection_id: this.prereq.conn.id, tables: this.prereq.tables.split(',').map(s => s.trim()).filter(Boolean) }; },
        async runPrereq() { this.prereq.busy = true; try { this.prereq.result = await api.connectors.prerequisites(this.prereqBody()); } catch (e) { store.error(e.message); } finally { this.prereq.busy = false; } },
        async enableCapture() {
            if (!await store.ask(`Enable change capture on ${this.prereq.conn.id} (database level) and on: ${this.prereq.tables || '(no tables)'}?`, { okLabel: 'Enable' })) return;
            this.prereq.busy = true;
            try {
                const db = await api.connectors.enableDb(this.prereqBody());
                store.success(db.message);
                if (this.prereqBody().tables.length) { const t = await api.connectors.enableTables(this.prereqBody()); t.results.forEach(r => r.success ? store.success(r.message) : store.error(r.message)); }
                await this.runPrereq();
            } catch (e) { store.error(e.message); } finally { this.prereq.busy = false; }
        },
        async disableCapture() {
            if (!this.prereqBody().tables.length) { store.warn('List the tables first'); return; }
            if (!await store.ask(`Disable capture on ${this.prereq.tables}?`, { danger: true, okLabel: 'Disable' })) return;
            const t = await store.run(() => api.connectors.disableTables(this.prereqBody()));
            if (t) t.results.forEach(r => r.success ? store.success(r.message) : store.error(r.message));
        },
        // ---- table browser
        async openBrowser(c) {
            this.browser = { open: true, conn: c, tables: [], columns: {}, selected: null, busy: true, filter: '', status: null };
            try {
                const [tables, status] = await Promise.all([api.connectors.tables({ connection_id: c.id }), api.connectors.captureStatus({ connection_id: c.id }).catch(() => null)]);
                this.browser.tables = tables; this.browser.status = status;
            } catch (e) { store.error(e.message); } finally { this.browser.busy = false; }
        },
        async showColumns(t) {
            this.browser.selected = t.key;
            if (this.browser.columns[t.key]) return;
            try { const r = await api.connectors.columns({ connection_id: this.browser.conn.id, tables: [t.key] }); this.browser.columns[t.key] = r[t.key]; }
            catch (e) { store.error(e.message); }
        },
        useInPrereq() { const sel = this.browser.selected; const c = this.browser.conn; this.browser.open = false; this.openPrereq(c); this.prereq.tables = sel || ''; },
        async showBroker() { this.broker = await store.run(() => api.connectors.brokerScript({})); },
    },
    template: `<div>
        <help-hint id="connections" title="Step 1 - register sources and destinations">
            A connection is a reusable login to one database; the same entry can act as a CDC <em>source</em> or as a <em>destination</em>. Flow per entry: <strong>Test</strong> (login works) →
            <strong>Prerequisites</strong> (is CDC / logical replication / binlog / replica set / LogMiner configured, and does the user have the rights - each failed check comes with the fix) →
            <strong>Browse</strong> (tables, columns, capture status). Passwords may be given as <code>\${env:NAME}</code> references so nothing secret lands in the store.
            For SQL Server use the <em>push-mode script</em> so the reader is woken by Service Broker instead of polling.
        </help-hint>
        <div class="toolbar">
            <button class="btn btn-primary btn-sm" @click="openNew()" data-tour="connections-new"><i class="bi bi-plus-lg"></i> New connection</button>
            <button class="btn btn-outline-secondary btn-sm" @click="showBroker()" data-tour="connections-push"><i class="bi bi-code-square"></i> SQL Server push-mode script</button>
            <span class="muted small ms-auto">{{ rows.length }} connections · secrets are stored by reference (<code>\${env:NAME}</code>) or encrypted at rest by your platform</span>
        </div>
        <div class="card"><div class="card-body">
            <data-table :columns="columns" :rows="rows" row-key="id" empty-text="No connections yet. Create one to validate CDC prerequisites.">
                <template #cell-id="{ row }"><strong>{{ row.id }}</strong><div class="small muted">{{ row.name }}</div></template>
                <template #cell-engine="{ row }"><span class="badge bg-dark">{{ row.engine }}</span></template>
                <template #cell-auth="{ row }"><span class="badge" :class="row.use_windows_auth ? 'bg-primary' : 'bg-secondary'">{{ row.use_windows_auth ? 'Windows' : (row.username || '–') }}</span></template>
                <template #cell-actions="{ row }">
                    <div class="btn-group btn-group-sm">
                        <button class="btn btn-outline-secondary" title="Test" @click="testById(row)"><i class="bi bi-plug"></i></button>
                        <button class="btn btn-outline-secondary" title="Browse tables" @click="openBrowser(row)"><i class="bi bi-table"></i></button>
                        <button class="btn btn-outline-secondary" title="CDC prerequisites" @click="openPrereq(row)"><i class="bi bi-clipboard-check"></i></button>
                        <button class="btn btn-outline-secondary" title="Edit" @click="openEdit(row)"><i class="bi bi-pencil"></i></button>
                        <button class="btn btn-outline-danger" title="Delete" @click="remove(row)"><i class="bi bi-trash"></i></button>
                    </div>
                </template>
            </data-table>
        </div></div>

        <!-- editor -->
        <modal-dialog v-if="editing" :title="editMode ? 'Edit connection ' + editing.id : 'New connection'" size="lg" @close="editing = null">
            <help-hint id="connection-editor" icon="bi-info-circle">
                The <em>engine</em> decides which CDC mechanism and which prerequisites apply; engine-specific options appear below the credentials. The id is what pipelines and mappings refer to, so keep it stable.
                Prefer a secret reference over a plain password, and a read-only / replica endpoint for snapshot sources. <strong>Test</strong> only checks the login - run <strong>Prerequisites</strong> from the list afterwards.
            </help-hint>
            <div class="row g-3">
                <div class="col-md-4"><label class="form-label">Engine</label>
                    <select v-model="editing.engine" class="form-select" :disabled="editMode" @change="onEngineChange"><option v-for="(d, k) in ENGINES" :key="k" :value="k">{{ d.label }}</option></select></div>
                <div class="col-md-4"><label class="form-label">Connection id</label><input v-model="editing.id" class="form-control" :disabled="editMode" placeholder="slug of the name if empty"></div>
                <div class="col-md-4"><label class="form-label">Display name *</label><input v-model="editing.name" class="form-control"></div>
                <div class="col-md-5"><label class="form-label">Server / host</label><input v-model="editing.server" class="form-control" placeholder="host or host\\\\instance"></div>
                <div class="col-md-2"><label class="form-label">Port</label><input v-model.number="editing.port" type="number" class="form-control" :placeholder="engineDef.port || ''"></div>
                <div class="col-md-5"><label class="form-label">Database</label><input v-model="editing.database" class="form-control"></div>
                <div class="col-md-3" v-if="engineDef.windowsAuth"><label class="form-label">Authentication</label>
                    <div class="form-check mt-1"><input v-model="editing.use_windows_auth" type="checkbox" class="form-check-input" id="winauth"><label class="form-check-label" for="winauth">Windows / Kerberos</label></div></div>
                <div class="col-md-4" v-if="!editing.use_windows_auth"><label class="form-label">Username</label><input v-model="editing.username" class="form-control"></div>
                <div class="col-md-5" v-if="!editing.use_windows_auth"><label class="form-label">Password or secret reference</label><input v-model="editing.password" type="text" class="form-control mono" placeholder="\${env:CRM_PW} · \${file:/run/secrets/crm} · plain"></div>
                <div class="col-md-2"><label class="form-label">Login timeout (s)</label><input v-model.number="editing.login_timeout" type="number" class="form-control"></div>
                <template v-for="f in engineDef.fields" :key="f.key">
                    <div class="col-md-6" v-if="f.top">
                        <template v-if="f.type === 'bool'"><div class="form-check mt-4"><input v-model="editing[f.key]" type="checkbox" class="form-check-input" :id="'f-' + f.key"><label class="form-check-label" :for="'f-' + f.key">{{ f.label }}</label></div></template>
                        <template v-else><label class="form-label">{{ f.label }}</label><select v-if="f.type === 'select'" v-model="editing[f.key]" class="form-select"><option v-for="o in f.options" :key="o" :value="o">{{ o || '(default)' }}</option></select></template>
                    </div>
                </template>
            </div>
            <hr>
            <h6 class="muted small text-uppercase">Engine options</h6>
            <div class="row g-3">
                <template v-for="f in engineDef.fields" :key="f.key">
                    <div class="col-md-6" v-if="!f.top">
                        <label class="form-label">{{ f.label }}</label>
                        <select v-if="f.type === 'select'" v-model="editing.options[f.key]" class="form-select"><option v-for="o in f.options" :key="o" :value="o">{{ o || '(default)' }}</option></select>
                        <input v-else-if="f.type === 'number'" v-model.number="editing.options[f.key]" type="number" step="any" class="form-control">
                        <textarea v-else-if="f.type === 'json'" v-model="optionsJsonText[f.key]" class="form-control mono" rows="2"></textarea>
                        <input v-else v-model="editing.options[f.key]" class="form-control">
                        <div v-if="f.hint" class="form-text">{{ f.hint }}</div>
                    </div>
                </template>
                <div class="col-12 muted small" v-if="!engineDef.fields.some(f => !f.top)">No engine specific options.</div>
            </div>
            <div v-if="testResult" class="alert mt-3 mb-0" :class="testResult.success ? 'alert-success' : 'alert-danger'">
                <i class="bi" :class="testResult.success ? 'bi-check-circle' : 'bi-x-circle'"></i> {{ testResult.message }}
                <div v-if="testResult.capabilities" class="small mt-1">capabilities: {{ testResult.capabilities.join(', ') }}</div>
            </div>
            <template #footer>
                <button class="btn btn-outline-secondary" :disabled="busy" @click="test()"><span v-if="busy" class="spinner-border spinner-border-sm"></span> Test</button>
                <button class="btn btn-secondary" @click="editing = null">Cancel</button>
                <button class="btn btn-primary" @click="save()">Save</button>
            </template>
        </modal-dialog>

        <!-- prerequisites -->
        <modal-dialog v-if="prereq.open" :title="'CDC prerequisites – ' + prereq.conn.id" size="lg" @close="prereq.open = false">
            <div class="row g-2 align-items-end">
                <div class="col-md-8"><label class="form-label">Tables to check (schema.table, comma separated)</label><input v-model="prereq.tables" class="form-control" placeholder="dbo.Customers, dbo.Orders"></div>
                <div class="col-md-4 d-flex gap-2">
                    <button class="btn btn-primary" :disabled="prereq.busy" @click="runPrereq()"><span v-if="prereq.busy" class="spinner-border spinner-border-sm"></span> Check</button>
                    <button class="btn btn-outline-warning" :disabled="prereq.busy" @click="enableCapture()">Enable capture</button>
                    <button class="btn btn-outline-danger" :disabled="prereq.busy" @click="disableCapture()">Disable</button>
                </div>
            </div>
            <div v-if="prereq.result" class="mt-3">
                <div class="mb-2">
                    <span class="state-badge" :class="prereq.result.ready ? 'state-ok' : 'state-failed'">{{ prereq.result.ready ? 'READY' : 'NOT READY' }}</span>
                    <span class="ms-2 muted small">{{ prereq.result.engine }} · {{ prereq.result.failed }} failed · {{ prereq.result.warnings }} warnings</span>
                </div>
                <div v-for="c in prereq.result.checks" :key="c.name" class="check-row">
                    <div class="ic">{{ c.ok ? '✅' : (c.severity === 'error' ? '❌' : (c.severity === 'warning' ? '⚠️' : 'ℹ️')) }}</div>
                    <div class="flex-grow-1">
                        <div><span class="mono">{{ c.name }}</span> <span class="muted small">{{ c.detail }}</span></div>
                        <div v-if="!c.ok && c.fix_hint" class="small mono text-warning-emphasis">↳ {{ c.fix_hint }}</div>
                    </div>
                </div>
            </div>
            <div v-else class="muted mt-3 small">Runs the engine specific checklist (edition, agent, permissions, retention, replica identity, supplemental logging, oplog window …) without changing anything.</div>
        </modal-dialog>

        <!-- table browser -->
        <modal-dialog v-if="browser.open" :title="'Tables – ' + browser.conn.id" size="xl" @close="browser.open = false">
            <div v-if="browser.status" class="small mb-2">capture: <span class="state-badge" :class="browser.status.cdc_enabled ? 'state-ok' : 'state-failed'">{{ browser.status.cdc_enabled ? 'enabled' : 'disabled' }}</span>
                <span class="muted ms-2 mono">{{ browser.status.max_lsn ? 'max lsn ' + browser.status.max_lsn : (browser.status.current_wal_lsn ? 'wal ' + browser.status.current_wal_lsn : (browser.status.current_scn ? 'scn ' + browser.status.current_scn : '')) }}</span></div>
            <div v-if="browser.busy" class="muted"><span class="spinner-border spinner-border-sm"></span> loading…</div>
            <div class="row g-3" v-else>
                <div class="col-md-6">
                    <input v-model="browser.filter" class="form-control form-control-sm mb-2" placeholder="filter tables…">
                    <div style="max-height: 480px; overflow:auto">
                        <table class="table table-sm table-hover mb-0">
                            <thead><tr><th>Table</th><th>CDC</th><th class="text-end">Rows</th><th class="text-end">Size</th></tr></thead>
                            <tbody><tr v-for="t in browser.tables.filter(t => !browser.filter || t.key.toLowerCase().includes(browser.filter.toLowerCase()))" :key="t.key" class="row-clickable" :class="{ 'table-active': browser.selected === t.key }" @click="showColumns(t)">
                                <td class="mono">{{ t.key }}</td><td><i class="bi" :class="t.cdc_enabled ? 'bi-check-circle-fill text-success' : 'bi-circle muted'"></i></td>
                                <td class="text-end">{{ fmt.num(t.row_estimate) }}</td><td class="text-end muted">{{ fmt.bytes(t.size_bytes) }}</td>
                            </tr></tbody>
                        </table>
                    </div>
                </div>
                <div class="col-md-6">
                    <div v-if="!browser.selected" class="muted">Select a table to see its columns.</div>
                    <template v-else>
                        <div class="d-flex justify-content-between mb-2"><strong class="mono">{{ browser.selected }}</strong><button class="btn btn-sm btn-outline-primary" @click="useInPrereq()">Check prerequisites</button></div>
                        <table class="table table-sm mb-0" v-if="browser.columns[browser.selected]">
                            <thead><tr><th>#</th><th>Column</th><th>Type</th><th>Null</th><th>PK</th></tr></thead>
                            <tbody><tr v-for="c in browser.columns[browser.selected]" :key="c.name"><td class="muted">{{ c.ordinal }}</td><td class="mono">{{ c.name }}</td><td>{{ c.data_type }}</td><td>{{ c.nullable ? 'yes' : 'no' }}</td><td>{{ c.is_primary_key ? '🔑' : '' }}</td></tr></tbody>
                        </table>
                        <div v-else class="muted"><span class="spinner-border spinner-border-sm"></span></div>
                    </template>
                </div>
            </div>
        </modal-dialog>

        <modal-dialog v-if="broker" title="SQL Server push mode – Service Broker notifier" size="lg" @close="broker = null">
            <p class="small">{{ broker.usage }}</p>
            <json-view :value="broker.sql" max-height="480px"></json-view>
        </modal-dialog>
    </div>`,
};

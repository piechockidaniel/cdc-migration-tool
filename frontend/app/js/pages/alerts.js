import { api } from '../api.js';
import { store, fmt } from '../store.js';

const emptyRule = () => ({ id: '', name: '', condition: '', severity: 'warning', description: '', rule_set: 'custom', cooldown_seconds: 900, for_seconds: 0, enabled: true, scope: 'global', labels: {}, message: '' });

export const Alerts = {
    data() {
        return {
            store, fmt, tab: 'alerts', ruleSets: [], rule: null, ruleIsNew: true, validation: null, context: null, contextOpen: false,
            channels: { webhook_url: '', email: { host: '', port: 25, sender: '', recipients: '', username: '', password: '', use_tls: true }, useEmail: false },
            ruleFilter: '', busy: false,
        };
    },
    computed: {
        rules() { const q = this.ruleFilter.toLowerCase(); return (store.alerts.rules || []).filter(r => !q || r.id.includes(q) || r.name.toLowerCase().includes(q) || r.rule_set.includes(q)).sort((a, b) => (a.rule_set + a.id > b.rule_set + b.id ? 1 : -1)); },
        enabledSets() { const e = store.alerts.enabled_rule_sets; return e === 'all' || e == null ? null : e; },
    },
    async created() { await this.load(); },
    methods: {
        async load() { try { this.ruleSets = await api.alerts.ruleSets(); const a = await api.alerts.snapshot(); store.alerts = a; } catch (e) { store.error(e.message); } },
        setEnabled(id) { const e = this.enabledSets; return e === null || e.includes(id); },
        async toggleSet(id) {
            const all = this.ruleSets.map(s => s.id);
            let enabled = this.enabledSets === null ? [...all] : [...this.enabledSets];
            enabled = enabled.includes(id) ? enabled.filter(x => x !== id) : [...enabled, id];
            await store.run(() => api.alerts.setRuleSets(enabled.length === all.length ? null : enabled)); await this.load();
        },
        newRule() { this.rule = emptyRule(); this.ruleIsNew = true; this.validation = null; },
        editRule(r) { this.rule = { ...emptyRule(), ...JSON.parse(JSON.stringify(r)) }; this.ruleIsNew = false; this.validation = null; },
        async validate() { this.validation = await store.run(() => api.alerts.validate(this.rule.condition)); },
        async saveRule() {
            if (!this.rule.id || !this.rule.name || !this.rule.condition) { store.warn('id, name and condition are required'); return; }
            const r = await store.run(() => api.alerts.saveRule(this.rule), { successMessage: 'Rule saved' });
            if (r) { this.rule = null; await this.load(); }
        },
        async deleteRule(r) {
            if (!await store.ask(`Remove rule "${r.id}"? Built-in rules are restored to their default.`, { danger: true, okLabel: 'Remove' })) return;
            await store.run(() => api.alerts.deleteRule(r.id), { successMessage: 'Rule removed' }); await this.load();
        },
        async toggleRule(r) { await store.run(() => api.alerts.saveRule({ ...r, enabled: !r.enabled })); await this.load(); },
        async evaluate() { const r = await store.run(() => api.alerts.evaluate()); if (r) { store.success(`evaluated – ${r.fired.length} alert(s) fired`); this.context = r.contexts; await this.load(); } },
        async showContext() { this.context = await store.run(() => api.alerts.context()); this.contextOpen = true; },
        async testChannels() { const r = await store.run(() => api.alerts.test()); if (r) store.success('test notification sent to: ' + r.sent_to.join(', ')); },
        async saveChannels() {
            const c = this.channels;
            const body = { webhook_url: c.webhook_url || null, email: c.useEmail && c.email.host ? { ...c.email, recipients: c.email.recipients.split(',').map(s => s.trim()).filter(Boolean) } : null };
            const r = await store.run(() => api.alerts.channels(body), { successMessage: 'Channels saved' }); if (r) await this.load();
        },
        sevIcon(s) { return { critical: 'bi-exclamation-octagon-fill', error: 'bi-x-octagon', warning: 'bi-exclamation-triangle', info: 'bi-info-circle' }[s] || 'bi-bell'; },
    },
    template: `<div>
        <help-hint id="alerts" title="Alerting - rules, rule sets and channels">
            Every ~30 s the engine collects <em>facts</em> (per-pipeline lag, failure rate, idle time, DLQ size, snapshot progress, source reachability) and evaluates the enabled <strong>rules</strong> against them; a rule fires after its condition held for <em>for_seconds</em> and resolves automatically when it clears.
            <strong>Rule sets</strong> bundle rules per environment (e.g. "production-strict"), <strong>channels</strong> deliver to webhook / Slack / e-mail / log. "Show facts" displays the exact values the rules see - use it when writing a new condition. Alerts also appear as toasts in the UI and in the audit trail.
        </help-hint>
        <ul class="nav tabs-clean mb-3" data-tour="alerts-tabs">
            <li class="nav-item"><a class="nav-link" :class="{ active: tab === 'alerts' }" href="#" @click.prevent="tab = 'alerts'"><i class="bi bi-bell"></i> Alerts <span v-if="store.alerts.active.length" class="badge bg-danger ms-1">{{ store.alerts.active.length }}</span></a></li>
            <li class="nav-item"><a class="nav-link" :class="{ active: tab === 'rules' }" href="#" @click.prevent="tab = 'rules'"><i class="bi bi-list-check"></i> Rules & rule sets</a></li>
            <li class="nav-item"><a class="nav-link" :class="{ active: tab === 'channels' }" href="#" @click.prevent="tab = 'channels'"><i class="bi bi-send"></i> Channels</a></li>
        </ul>

        <!-- alerts -->
        <div v-if="tab === 'alerts'">
            <div class="toolbar">
                <button class="btn btn-outline-primary btn-sm" @click="evaluate()"><i class="bi bi-play"></i> Evaluate now</button>
                <button class="btn btn-outline-secondary btn-sm" @click="showContext()"><i class="bi bi-braces"></i> Show facts</button>
                <span class="muted small ms-auto">last evaluation: {{ store.alerts.last_evaluation ? fmt.ago(new Date(store.alerts.last_evaluation * 1000).toISOString()) : 'never' }} · every {{ store.alerts.interval || 30 }} s in the background</span>
            </div>
            <div class="row g-3">
                <div class="col-lg-7">
                    <div class="card mb-3"><div class="card-header"><span>Active</span></div><div class="card-body">
                        <empty-state v-if="!store.alerts.active.length" icon="bi-emoji-smile" text="No active alerts."></empty-state>
                        <div v-for="a in store.alerts.active" :key="a.rule_id + a.scope_key" class="sev-bar mb-3" :class="'sev-' + a.severity">
                            <div class="d-flex align-items-center gap-2"><i class="bi" :class="sevIcon(a.severity)"></i><state-badge :state="a.severity"></state-badge><strong>{{ a.name }}</strong><state-badge :state="a.status"></state-badge><span class="muted small mono ms-auto">{{ a.scope_key }}</span></div>
                            <div class="small mt-1">{{ a.message }}</div>
                            <div class="small muted">since {{ fmt.ts(a.first_seen_iso) }} · last seen {{ fmt.ago(a.last_seen_iso) }} · notified {{ a.notifications }}×</div>
                            <details v-if="a.facts && Object.keys(a.facts).length"><summary class="small muted">facts at last evaluation</summary><facts-tree :value="a.facts"></facts-tree></details>
                        </div>
                    </div></div>
                </div>
                <div class="col-lg-5">
                    <div class="card"><div class="card-header"><span>Recently resolved</span></div><div class="card-body">
                        <div v-if="!store.alerts.recent_resolved.length" class="muted">Nothing resolved recently.</div>
                        <div v-for="a in [...store.alerts.recent_resolved].reverse().slice(0, 20)" :key="a.rule_id + a.scope_key + a.first_seen" class="small sev-bar mb-2" :class="'sev-' + a.severity">
                            <strong>{{ a.name }}</strong> <span class="muted mono">{{ a.scope_key }}</span><div class="muted">{{ fmt.ts(a.first_seen_iso) }} → {{ fmt.ts(a.last_seen_iso) }}</div>
                        </div>
                    </div></div>
                </div>
            </div>
        </div>

        <!-- rules -->
        <div v-if="tab === 'rules'">
            <div class="row g-3">
                <div class="col-lg-4">
                    <div class="card mb-3"><div class="card-header"><span>Rule sets</span></div><div class="card-body">
                        <div v-for="s in ruleSets" :key="s.id" class="form-check form-switch mb-2">
                            <input class="form-check-input" type="checkbox" :id="'rs-' + s.id" :checked="setEnabled(s.id)" @change="toggleSet(s.id)">
                            <label class="form-check-label" :for="'rs-' + s.id"><strong>{{ s.id }}</strong><div class="small muted">{{ s.description }} · {{ s.rules.length }} rules</div></label>
                        </div>
                        <div class="small muted mt-2">Facts available to conditions: <code>pipeline.*</code>, <code>source.*</code>, <code>failures.*</code>, <code>snapshot.*</code> – <a href="#" @click.prevent="showContext()">show current values</a>.</div>
                    </div></div>
                </div>
                <div class="col-lg-8">
                    <div class="card"><div class="card-header"><span>Rules ({{ rules.length }})</span><div class="d-flex gap-2"><input v-model="ruleFilter" class="form-control form-control-sm" placeholder="filter…"><button class="btn btn-sm btn-primary text-nowrap" @click="newRule()"><i class="bi bi-plus-lg"></i> Rule</button></div></div>
                    <div class="card-body p-0"><table class="table table-sm mb-0">
                        <thead><tr><th></th><th>Rule</th><th>Condition</th><th>Set / scope</th><th></th></tr></thead>
                        <tbody><tr v-for="r in rules" :key="r.id" :class="{ 'opacity-50': !r.enabled }">
                            <td><div class="form-check form-switch mb-0"><input class="form-check-input" type="checkbox" :checked="r.enabled" @change="toggleRule(r)"></div></td>
                            <td><state-badge :state="r.severity"></state-badge> <strong>{{ r.name }}</strong><div class="small muted mono">{{ r.id }}</div><div v-if="store.alerts.evaluation_errors && store.alerts.evaluation_errors[r.id]" class="small text-danger">{{ store.alerts.evaluation_errors[r.id] }}</div></td>
                            <td class="mono small">{{ r.condition }}<div class="muted" v-if="r.for_seconds || r.cooldown_seconds">for {{ r.for_seconds }}s · cooldown {{ r.cooldown_seconds }}s</div></td>
                            <td class="small">{{ r.rule_set }}<div class="muted">{{ r.scope }}</div></td>
                            <td class="text-end"><div class="btn-group btn-group-sm"><button class="btn btn-outline-secondary" @click="editRule(r)"><i class="bi bi-pencil"></i></button><button class="btn btn-outline-danger" @click="deleteRule(r)"><i class="bi bi-trash"></i></button></div></td>
                        </tr></tbody>
                    </table></div></div>
                </div>
            </div>
        </div>

        <!-- channels -->
        <div v-if="tab === 'channels'">
            <div class="card" style="max-width: 760px"><div class="card-header"><span>Notification channels</span><span class="muted small">active: {{ (store.alerts.channels || []).join(', ') }}</span></div><div class="card-body">
                <div class="mb-3"><label class="form-label">Webhook URL (Slack / Teams / Alertmanager / generic JSON POST)</label><input v-model="channels.webhook_url" class="form-control mono" placeholder="https://hooks.slack.com/services/…"></div>
                <div class="form-check form-switch mb-2"><input v-model="channels.useEmail" class="form-check-input" type="checkbox" id="ch-mail"><label class="form-check-label" for="ch-mail">E-mail (SMTP)</label></div>
                <div class="row g-2" v-if="channels.useEmail">
                    <div class="col-md-6"><label class="form-label">SMTP host</label><input v-model="channels.email.host" class="form-control"></div>
                    <div class="col-md-2"><label class="form-label">Port</label><input v-model.number="channels.email.port" type="number" class="form-control"></div>
                    <div class="col-md-4 d-flex align-items-end"><div class="form-check"><input v-model="channels.email.use_tls" class="form-check-input" type="checkbox" id="ch-tls"><label class="form-check-label" for="ch-tls">STARTTLS</label></div></div>
                    <div class="col-md-6"><label class="form-label">Sender</label><input v-model="channels.email.sender" class="form-control"></div>
                    <div class="col-md-6"><label class="form-label">Recipients (comma separated)</label><input v-model="channels.email.recipients" class="form-control"></div>
                    <div class="col-md-6"><label class="form-label">Username</label><input v-model="channels.email.username" class="form-control"></div>
                    <div class="col-md-6"><label class="form-label">Password / secret ref</label><input v-model="channels.email.password" class="form-control mono"></div>
                </div>
                <div class="mt-3 d-flex gap-2"><button class="btn btn-primary" @click="saveChannels()">Save</button><button class="btn btn-outline-secondary" @click="testChannels()"><i class="bi bi-send"></i> Send test</button></div>
                <div class="small muted mt-2">Log, audit trail and the in-app (WebSocket) channel are always active. Set <code>CDCSYNC_ALERT_WEBHOOK</code> to configure the webhook by environment.</div>
            </div></div>
        </div>

        <!-- rule editor -->
        <modal-dialog v-if="rule" :title="ruleIsNew ? 'New rule' : 'Rule ' + rule.id" size="lg" @close="rule = null">
            <help-hint id="rule-editor" icon="bi-info-circle">
                A condition is a small boolean expression over the facts (see "Show facts" for the names), evaluated per pipeline / snapshot / source as the scope dictates, e.g. <code>pipeline.lag_seconds &gt; 300 and pipeline.running</code> or <code>dlq.unresolved &gt; 100</code>.
                <em>For seconds</em> avoids flapping; <em>cooldown</em> limits re-notification. Validate runs the expression against the live facts right away.
            </help-hint>
            <div class="row g-3">
                <div class="col-md-4"><label class="form-label">Id</label><input v-model="rule.id" class="form-control mono" :disabled="!ruleIsNew"></div>
                <div class="col-md-8"><label class="form-label">Name</label><input v-model="rule.name" class="form-control"></div>
                <div class="col-12"><label class="form-label">Condition</label><textarea v-model="rule.condition" class="form-control mono" rows="2" placeholder="pipeline.lag_seconds > 300 and pipeline.running"></textarea>
                    <div class="d-flex gap-2 mt-1 align-items-center"><button class="btn btn-sm btn-outline-secondary" @click="validate()">Validate against current facts</button>
                        <span v-if="validation" class="small" :class="validation.valid ? 'text-success' : 'text-danger'">{{ validation.valid ? 'valid – ' + Object.entries(validation.results).map(([k, v]) => k + ': ' + (v ? 'FIRES' : 'quiet')).join(', ') : validation.error }}</span></div></div>
                <div class="col-md-3"><label class="form-label">Severity</label><select v-model="rule.severity" class="form-select"><option>info</option><option>warning</option><option>error</option><option>critical</option></select></div>
                <div class="col-md-3"><label class="form-label">Scope</label><select v-model="rule.scope" class="form-select"><option value="global">global</option><option value="per_pipeline">per pipeline</option><option value="per_snapshot">per snapshot</option></select></div>
                <div class="col-md-3"><label class="form-label">Rule set</label><input v-model="rule.rule_set" class="form-control" list="rulesets"><datalist id="rulesets"><option v-for="s in ruleSets" :key="s.id" :value="s.id"></option><option value="custom"></option></datalist></div>
                <div class="col-md-3 d-flex align-items-end"><div class="form-check form-switch"><input v-model="rule.enabled" class="form-check-input" type="checkbox" id="r-en"><label class="form-check-label" for="r-en">enabled</label></div></div>
                <div class="col-md-3"><label class="form-label">Must hold for (s)</label><input v-model.number="rule.for_seconds" type="number" class="form-control"></div>
                <div class="col-md-3"><label class="form-label">Cooldown (s)</label><input v-model.number="rule.cooldown_seconds" type="number" class="form-control"></div>
                <div class="col-md-6"><label class="form-label">Message template</label><input v-model="rule.message" class="form-control" placeholder="lag is {pipeline[lag_seconds]} s"></div>
                <div class="col-12"><label class="form-label">Description</label><input v-model="rule.description" class="form-control"></div>
            </div>
            <template #footer><button class="btn btn-secondary" @click="rule = null">Cancel</button><button class="btn btn-primary" @click="saveRule()">Save</button></template>
        </modal-dialog>

        <modal-dialog v-if="contextOpen && context" title="Current facts (rule engine input)" size="lg" @close="contextOpen = false"><facts-tree :value="context"></facts-tree></modal-dialog>
    </div>`,
};

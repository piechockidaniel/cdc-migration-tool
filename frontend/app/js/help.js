/* Context help: dismissible per-page hints and a guided first-run tour.
 *
 * Hints   - <help-hint id="..." title="...">text</help-hint> on each page. Dismissed ids live in localStorage
 *           (`help.hints`), "Show hints again" in the topbar help menu resets them.
 * Tour    - a spotlight overlay walking through the flow Connections -> Mappings -> Pipelines -> Snapshots ->
 *           Alerts -> Failures -> Audit/Metrics. Elements are located by `data-tour="<name>"` attributes so the
 *           page templates stay free of tour logic. Auto-starts once on an empty instance (no connections). */
const { reactive, nextTick } = Vue;

const LS_HINTS = 'help.hints';
const LS_TOUR = 'help.tourDone';

function readJson(key, fallback) { try { return JSON.parse(localStorage.getItem(key)) ?? fallback; } catch { return fallback; } }

export const TOUR_STEPS = [
    { route: null, target: 'nav', placement: 'right', title: 'Welcome to CDC Synchronizer',
      text: 'The sidebar follows the order of work: define Connections, describe Mappings, run Pipelines, backfill with Snapshots - then watch Alerts, Failures, the Audit trail and Metrics. This tour walks through each step; you can reopen it any time from the ? menu.' },
    { route: '/dashboard', target: 'topbar-status', placement: 'bottom', title: 'Live status',
      text: '"live" means the browser receives alerts and change events over WebSocket; "healthy" is the /health verdict (store, pipelines, lag); the bell counts active alerts. Everything else on the dashboard is a 24h summary.' },
    { route: '/connections', target: 'connections-new', placement: 'bottom', title: '1. Connections',
      text: 'Register every source and destination once (SQL Server, PostgreSQL, MariaDB/MySQL, MongoDB, Oracle, SQLite, or the built-in Demo source). Test the login, then run "Prerequisites" - it tells you exactly which CDC switch, privilege or log setting is missing and how to fix it, before anything is started.' },
    { route: '/connections', target: 'connections-push', placement: 'bottom', title: 'Push instead of poll',
      text: 'SQL Server has no change stream, so by default the reader checks a cheap watermark with exponential back-off. This script installs a Service Broker queue that wakes the reader the moment a change is committed - no hammering of the source.' },
    { route: '/mappings', target: 'mappings-new', placement: 'bottom', title: '2. Mappings',
      text: 'A mapping says which source table lands where and how. "table" mappings are applied change-by-change (rename, ignore, default, auto-generate, DuckDB expressions, per-batch DuckDB scripts); "sql" mappings re-run a query. Multi-source merges and the visual designer live in the legacy UI.' },
    { route: '/pipelines', target: 'pipelines-new', placement: 'bottom', title: '3. Pipelines',
      text: 'A pipeline = one source connection + one destination + a set of mappings. It enables capture, reads the change stream, applies batches and commits the position only after the destination transaction succeeded - so a crash never loses or double-applies a change.' },
    { route: '/snapshots', target: 'snapshots-new', placement: 'bottom', title: '4. Snapshots (initial load)',
      text: 'Never SELECT * a 16 TB table. A snapshot job walks the primary key in small chunks, upserts each chunk, can be paused, throttled or restricted to a maintenance window, and resumes from the last key after a restart. Start the pipeline first so changes made during the copy are not lost.' },
    { route: '/alerts', target: 'alerts-tabs', placement: 'bottom', title: '5. Alerts, rules and rule sets',
      text: 'Rules evaluate facts (lag, failure rate, idle source, DLQ growth, snapshot stalls) on every tick. Rule sets bundle them per environment; channels deliver to webhook, Slack, e-mail or the log. Alerts auto-resolve when the condition clears.' },
    { route: '/failures', target: 'failures-tabs', placement: 'bottom', title: '6. Failures: patterns and the dead-letter queue',
      text: 'Failed rows never block the stream - they are parked in the DLQ with their error. The pattern miner groups them by normalised error signature and suggests the fix (widen a column, add a default, map a rename, drop a legacy table). Replay after fixing, or resolve as deprecated.' },
    { route: '/audit', target: 'audit-filters', placement: 'bottom', title: '7. Audit trail and metrics',
      text: 'Every lifecycle event, batch, config change and alert is recorded here (export to CSV for auditors). Metrics shows the Prometheus counters and histories: events read/applied, round-trips to sources, apply latency, snapshot throughput.' },
    { route: '/settings', target: 'settings-instance', placement: 'bottom', title: 'Settings - you are done',
      text: 'Here you see where configuration and audit data are stored (SQLite by default, with optimistic locking for concurrent editors), the Kafka mirror state and the audit level. Start with a Demo connection if you want to see the whole flow without a database.' },
];

export const help = reactive({
    hidden: new Set(readJson(LS_HINTS, [])),
    showAll: false,
    tour: { active: false, step: 0, rect: null, placement: 'bottom', pending: false },
    menuOpen: false,

    // --------------------------------------------------------------- hints
    isHidden(id) { return !this.showAll && this.hidden.has(id); },
    dismiss(id) { this.hidden.add(id); localStorage.setItem(LS_HINTS, JSON.stringify([...this.hidden])); },
    resetHints() { this.hidden = new Set(); localStorage.removeItem(LS_HINTS); this.showAll = false; },
    get hintsDismissed() { return this.hidden.size; },

    // --------------------------------------------------------------- tour
    tourDone() { return localStorage.getItem(LS_TOUR) === '1'; },
    startTour(step = 0) { this.menuOpen = false; this.tour.active = true; this.goto(step); },
    endTour(markDone = true) {
        this.tour.active = false; this.tour.rect = null;
        if (markDone) localStorage.setItem(LS_TOUR, '1');
        window.removeEventListener('resize', this._onResize);
    },
    next() { if (this.tour.step >= TOUR_STEPS.length - 1) this.endTour(); else this.goto(this.tour.step + 1); },
    prev() { if (this.tour.step > 0) this.goto(this.tour.step - 1); },
    get current() { return TOUR_STEPS[this.tour.step]; },
    async goto(step) {
        this.tour.step = step;
        this.tour.pending = true;
        const s = TOUR_STEPS[step];
        if (s.route && location.hash.replace(/^#/, '').split('?')[0] !== s.route) location.hash = s.route;
        await nextTick();
        // wait for the page component (and its data) to render the anchor
        let el = null;
        for (let i = 0; i < 30 && !el; i++) { el = document.querySelector(`[data-tour="${s.target}"]`); if (!el) await new Promise(r => setTimeout(r, 50)); }
        this.tour.pending = false;
        if (!this.tour.active) return;
        this._place(el, s.placement);
        window.removeEventListener('resize', this._onResize);
        window.addEventListener('resize', this._onResize);
    },
    _onResize() { const s = TOUR_STEPS[help.tour.step]; help._place(document.querySelector(`[data-tour="${s.target}"]`), s.placement); },
    _place(el, placement) {
        if (!el) { this.tour.rect = null; this.tour.placement = 'center'; return; }
        el.scrollIntoView({ block: 'nearest', inline: 'nearest' });
        let r = el.getBoundingClientRect();
        if (el.hasAttribute('data-tour-fit') && el.children.length) {   // union of the children (e.g. a stretched <nav>)
            const rs = [...el.children].map(c => c.getBoundingClientRect()).filter(c => c.width && c.height);
            if (rs.length) r = { top: Math.min(...rs.map(c => c.top)), left: Math.min(...rs.map(c => c.left)), right: Math.max(...rs.map(c => c.right)), bottom: Math.max(...rs.map(c => c.bottom)) };
            r.width = r.right - r.left; r.height = r.bottom - r.top;
        }
        const pad = 6;
        this.tour.rect = { top: r.top - pad, left: r.left - pad, width: r.width + 2 * pad, height: r.height + 2 * pad };
        // keep the popover on screen
        let p = placement;
        if (p === 'bottom' && r.bottom + 220 > window.innerHeight) p = 'top';
        if (p === 'right' && r.right + 380 > window.innerWidth) p = 'bottom';
        this.tour.placement = p;
    },
    /* Auto-start once: empty instance (no connections) and the tour was never completed/skipped. */
    maybeAutoStart(store) {
        if (this.tourDone() || this.tour.active || /[?&]notour\b/.test(location.hash + location.search)) return;
        if (store.connections.length === 0 && store.lastRefresh) this.startTour(0);
    },
});

/* Dismissible info box. Usage: <help-hint id="connections" title="What happens here"> ...text... </help-hint> */
export const HelpHint = {
    props: { id: { type: String, required: true }, title: String, icon: { type: String, default: 'bi-lightbulb' }, tone: { type: String, default: 'info' } },
    data() { return { help }; },
    computed: { visible() { return !help.isHidden(this.id); } },
    template: `<div v-if="visible" class="help-hint" :class="'tone-' + tone" role="note">
        <i :class="'bi ' + icon"></i>
        <div class="help-hint-body"><div v-if="title" class="help-hint-title">{{ title }}</div><div class="help-hint-text"><slot></slot></div></div>
        <button class="btn-close" aria-label="Dismiss hint" title="Dismiss (Show hints again from the ? menu)" @click="help.dismiss(id)"></button>
    </div>`,
};

/* Spotlight overlay + popover; mounted once in index.html. */
export const TourOverlay = {
    data() { return { help, steps: TOUR_STEPS }; },
    computed: {
        t() { return help.tour; },
        step() { return help.current; },
        spotStyle() { const r = this.t.rect; return r ? { top: r.top + 'px', left: r.left + 'px', width: r.width + 'px', height: r.height + 'px' } : { display: 'none' }; },
        popStyle() {
            const r = this.t.rect, W = 360;
            if (!r || this.t.placement === 'center') return { top: '50%', left: '50%', transform: 'translate(-50%, -50%)' };
            const clampX = (x) => Math.max(8, Math.min(x, window.innerWidth - W - 8)) + 'px';
            if (this.t.placement === 'right') return { top: Math.max(8, r.top) + 'px', left: (r.left + r.width + 12) + 'px' };
            if (this.t.placement === 'top') return { bottom: (window.innerHeight - r.top + 12) + 'px', left: clampX(r.left) };
            return { top: (r.top + r.height + 12) + 'px', left: clampX(r.left) };
        },
    },
    mounted() { this._key = (e) => { if (!help.tour.active) return; if (e.key === 'Escape') help.endTour(); if (e.key === 'ArrowRight') help.next(); if (e.key === 'ArrowLeft') help.prev(); }; document.addEventListener('keydown', this._key); },
    unmounted() { document.removeEventListener('keydown', this._key); },
    template: `<teleport to="body">
        <div v-if="t.active" class="tour-layer">
            <div class="tour-backdrop" @click="help.endTour()"></div>
            <div class="tour-spot" :style="spotStyle"></div>
            <div class="tour-pop" :style="popStyle" role="dialog" aria-modal="true">
                <div class="tour-pop-head"><span class="tour-count">{{ t.step + 1 }} / {{ steps.length }}</span><strong>{{ step.title }}</strong><button class="btn-close" aria-label="Close tour" @click="help.endTour()"></button></div>
                <div class="tour-pop-text">{{ step.text }}</div>
                <div class="tour-pop-foot">
                    <button class="btn btn-sm btn-link text-decoration-none px-0" @click="help.endTour()">Skip tour</button>
                    <div class="ms-auto d-flex gap-2">
                        <button class="btn btn-sm btn-outline-secondary" :disabled="t.step === 0" @click="help.prev()">Back</button>
                        <button class="btn btn-sm btn-primary" @click="help.next()">{{ t.step === steps.length - 1 ? 'Finish' : 'Next' }}</button>
                    </div>
                </div>
                <div class="tour-dots"><span v-for="(s, i) in steps" :key="i" :class="{ on: i === t.step }" @click="help.goto(i)"></span></div>
            </div>
        </div>
    </teleport>`,
};

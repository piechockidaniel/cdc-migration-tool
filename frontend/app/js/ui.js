/* Shared UI components (registered globally in main.js). */
import { fmt } from './store.js';

export const KpiCard = {
    props: { label: String, value: [String, Number], sub: String, tone: { type: String, default: 'primary' }, icon: String },
    template: `<div class="kpi" :class="'tone-' + tone">
        <div class="kpi-label"><i v-if="icon" :class="'bi ' + icon + ' me-1'"></i>{{ label }}</div>
        <div class="kpi-value">{{ value }}</div>
        <div v-if="sub" class="kpi-sub">{{ sub }}</div>
    </div>`,
};

export const StateBadge = {
    props: { state: String },
    template: `<span class="state-badge" :class="'state-' + (state || 'unknown')">{{ state || 'unknown' }}</span>`,
};

export const ModalDialog = {
    props: { title: String, size: { type: String, default: '' } },
    emits: ['close'],
    mounted() { this._esc = (e) => { if (e.key === 'Escape') this.$emit('close'); }; document.addEventListener('keydown', this._esc); document.body.style.overflow = 'hidden'; },
    unmounted() { document.removeEventListener('keydown', this._esc); document.body.style.overflow = ''; },
    template: `<teleport to="body">
        <div class="modal-backdrop-custom" @mousedown.self="$emit('close')">
            <div class="modal-box" :class="size">
                <div class="mh"><span>{{ title }}</span><button class="btn-close" @click="$emit('close')"></button></div>
                <div class="mb"><slot></slot></div>
                <div class="mf" v-if="$slots.footer"><slot name="footer"></slot></div>
            </div>
        </div>
    </teleport>`,
};

export const EmptyState = {
    props: { icon: { type: String, default: 'bi-inbox' }, text: { type: String, default: 'Nothing here yet.' } },
    template: `<div class="empty"><i :class="'bi ' + icon"></i>{{ text }}<slot></slot></div>`,
};

export const JsonView = {
    props: { value: null, maxHeight: { type: String, default: '360px' } },
    computed: { text() { return typeof this.value === 'string' ? this.value : fmt.json(this.value); } },
    template: `<pre class="small-pre" :style="{ maxHeight }">{{ text }}</pre>`,
};

/* Generic table: columns = [{ key, label, sortable, class, format }], rows = objects.
   Per-column cell customisation via scoped slot `cell-<key>` ({ row, value }). */
export const DataTable = {
    props: {
        columns: Array, rows: Array, rowKey: { type: String, default: 'id' }, pageSize: { type: Number, default: 25 },
        searchable: { type: Boolean, default: true }, emptyText: { type: String, default: 'No rows.' }, dense: Boolean, clickable: Boolean,
    },
    emits: ['row-click'],
    data() { return { q: '', sortKey: null, sortDir: 1, page: 1 }; },
    computed: {
        filtered() {
            let rows = this.rows || [];
            if (this.q) {
                const q = this.q.toLowerCase();
                rows = rows.filter(r => this.columns.some(c => String(this.cell(r, c) ?? '').toLowerCase().includes(q)));
            }
            if (this.sortKey) {
                const k = this.sortKey, d = this.sortDir;
                rows = [...rows].sort((a, b) => { const x = a[k], y = b[k]; if (x == null) return 1; if (y == null) return -1; return (x > y ? 1 : x < y ? -1 : 0) * d; });
            }
            return rows;
        },
        pages() { return Math.max(1, Math.ceil(this.filtered.length / this.pageSize)); },
        paged() { const p = Math.min(this.page, this.pages); return this.filtered.slice((p - 1) * this.pageSize, p * this.pageSize); },
    },
    watch: { rows() { if (this.page > this.pages) this.page = this.pages; } },
    methods: {
        cell(row, col) { const v = row[col.key]; return col.format ? col.format(v, row) : v; },
        sort(col) { if (!col.sortable) return; if (this.sortKey === col.key) this.sortDir = -this.sortDir; else { this.sortKey = col.key; this.sortDir = 1; } },
    },
    template: `<div>
        <div class="d-flex justify-content-between align-items-center mb-2" v-if="searchable || pages > 1">
            <input v-if="searchable" v-model="q" class="form-control form-control-sm" style="max-width: 260px" placeholder="Filter…">
            <div v-if="pages > 1" class="d-flex align-items-center gap-2 small muted">
                <button class="btn btn-sm btn-outline-secondary" :disabled="page <= 1" @click="page--">‹</button>
                page {{ page }} / {{ pages }} ({{ filtered.length }} rows)
                <button class="btn btn-sm btn-outline-secondary" :disabled="page >= pages" @click="page++">›</button>
            </div>
        </div>
        <div class="table-responsive">
        <table class="table table-hover mb-0" :class="{ 'table-sm': dense }">
            <thead><tr>
                <th v-for="c in columns" :key="c.key" :class="[c.class, { sortable: c.sortable }]" @click="sort(c)">
                    {{ c.label }} <i v-if="sortKey === c.key" class="bi" :class="sortDir > 0 ? 'bi-caret-up-fill' : 'bi-caret-down-fill'"></i>
                </th>
            </tr></thead>
            <tbody>
                <tr v-for="row in paged" :key="row[rowKey]" :class="{ 'row-clickable': clickable }" @click="$emit('row-click', row)">
                    <td v-for="c in columns" :key="c.key" :class="c.class">
                        <slot :name="'cell-' + c.key" :row="row" :value="row[c.key]">{{ cell(row, c) }}</slot>
                    </td>
                </tr>
                <tr v-if="!paged.length"><td :colspan="columns.length"><div class="empty py-3 mb-0"><i class="bi bi-inbox"></i>{{ emptyText }}</div></td></tr>
            </tbody>
        </table>
        </div>
    </div>`,
};

/* Chart.js line chart. series = [{ label, data: [[ts, value], ...], color }] */
export const LineChart = {
    props: { series: Array, unit: { type: String, default: '' }, tall: Boolean, stacked: Boolean },
    mounted() { this.render(); },
    watch: { series: { deep: true, handler() { this.render(); } } },
    unmounted() { if (this.chart) this.chart.destroy(); },
    methods: {
        render() {
            const dark = document.documentElement.getAttribute('data-bs-theme') === 'dark';
            const grid = dark ? '#243044' : '#eef0f4', text = dark ? '#94a3b8' : '#6b7280';
            const palette = ['#2563eb', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#06b6d4', '#f97316', '#84cc16'];
            const datasets = (this.series || []).map((s, i) => ({
                label: s.label, data: (s.data || []).map(([t, v]) => ({ x: t * 1000, y: v })),
                borderColor: s.color || palette[i % palette.length], backgroundColor: (s.color || palette[i % palette.length]) + '22',
                borderWidth: 1.5, pointRadius: 0, tension: .25, fill: this.stacked,
            }));
            const unit = this.unit;
            if (this.chart) { this.chart.data.datasets = datasets; this.chart.update('none'); return; }
            this.chart = new Chart(this.$refs.canvas, {
                type: 'line', data: { datasets },
                options: {
                    animation: false, responsive: true, maintainAspectRatio: false, interaction: { mode: 'nearest', intersect: false },
                    plugins: { legend: { display: datasets.length > 1 && datasets.length < 9, labels: { color: text, boxWidth: 10 } },
                               tooltip: { callbacks: { label: (c) => `${c.dataset.label}: ${fmt.num(c.parsed.y, 2)} ${unit}` } } },
                    scales: {
                        x: { type: 'linear', ticks: { color: text, maxTicksLimit: 8, callback: (v) => new Date(v).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }) }, grid: { color: grid } },
                        y: { beginAtZero: true, stacked: this.stacked, ticks: { color: text, callback: (v) => fmt.num(v, 1) + (unit ? ' ' + unit : '') }, grid: { color: grid } },
                    },
                },
            });
        },
    },
    computed: { empty() { return !(this.series || []).some(s => s.data && s.data.length); } },
    template: `<div class="chart-box" :class="{ tall }"><canvas ref="canvas"></canvas><div v-if="empty" class="chart-empty">no data yet</div></div>`,
};

export const FactsTree = {
    props: { value: null, depth: { type: Number, default: 0 } },
    template: `<div class="facts-tree">
        <template v-if="value && typeof value === 'object' && !Array.isArray(value)">
            <div v-for="(v, k) in value" :key="k" :style="{ paddingLeft: (depth * 14) + 'px' }">
                <template v-if="v && typeof v === 'object' && !Array.isArray(v)"><span class="k">{{ k }}</span>:<facts-tree :value="v" :depth="depth + 1"></facts-tree></template>
                <template v-else><span class="k">{{ k }}</span>: <span>{{ Array.isArray(v) ? JSON.stringify(v) : String(v) }}</span></template>
            </div>
        </template>
        <span v-else>{{ String(value) }}</span>
    </div>`,
};

export const components = {
    'kpi-card': KpiCard, 'state-badge': StateBadge, 'modal-dialog': ModalDialog, 'empty-state': EmptyState,
    'json-view': JsonView, 'data-table': DataTable, 'line-chart': LineChart, 'facts-tree': FactsTree,
};

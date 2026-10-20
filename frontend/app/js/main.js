/* Application bootstrap: hash router, global components, polling and live feed. */
import { store } from './store.js';
import { components } from './ui.js';
import { help, HelpHint, TourOverlay } from './help.js';
import { Dashboard } from './pages/dashboard.js';
import { Connections } from './pages/connections.js';
import { Mappings } from './pages/mappings.js';
import { Pipelines } from './pages/pipelines.js';
import { Snapshots } from './pages/snapshots.js';
import { Alerts } from './pages/alerts.js';
import { Failures } from './pages/failures.js';
import { Audit } from './pages/audit.js';
import { Metrics } from './pages/metrics.js';
import { Settings } from './pages/settings.js';

const { createApp, reactive, computed } = Vue;

const routes = [
    { path: '/dashboard', label: 'Dashboard', icon: 'bi-speedometer2', component: Dashboard },
    { path: '/connections', label: 'Connections', icon: 'bi-plug', component: Connections },
    { path: '/mappings', label: 'Mappings', icon: 'bi-diagram-2', component: Mappings },
    { path: '/pipelines', label: 'Pipelines', icon: 'bi-diagram-3', component: Pipelines, badge: () => store.runningPipelines().length, badgeClass: 'bg-success' },
    { path: '/snapshots', label: 'Snapshots', icon: 'bi-layers', component: Snapshots, badge: () => store.snapshots.filter(j => j.state === 'running').length, badgeClass: 'bg-info' },
    { path: '/alerts', label: 'Alerts & Rules', icon: 'bi-bell', component: Alerts, badge: () => store.alerts.active.length },
    { path: '/failures', label: 'Failures', icon: 'bi-exclamation-octagon', component: Failures, badge: () => store.auditStats ? (store.auditStats.failures_total - store.auditStats.failures_resolved) : 0, badgeClass: 'bg-warning text-dark' },
    { path: '/audit', label: 'Audit trail', icon: 'bi-journal-text', component: Audit },
    { path: '/metrics', label: 'Metrics', icon: 'bi-graph-up', component: Metrics },
    { path: '/settings', label: 'Settings', icon: 'bi-gear', component: Settings },
];
// legacy bookmarks -> new pages
const aliases = { '/admin': '/connections', '/operations': '/pipelines', '/monitoring': '/dashboard', '/control': '/dashboard', '/': '/dashboard', '': '/dashboard' };

function parseHash() {
    const raw = location.hash.replace(/^#/, '') || '/dashboard';
    const [pathPart, queryPart] = raw.split('?');
    const segments = pathPart.split('/').filter(Boolean);
    let path = '/' + (segments[0] || 'dashboard');
    path = aliases[path] || path;
    const query = {};
    (queryPart || '').split('&').filter(Boolean).forEach(kv => { const [k, v] = kv.split('='); query[decodeURIComponent(k)] = decodeURIComponent(v || ''); });
    return { path, params: { id: segments[1] ? decodeURIComponent(segments[1]) : null, sub: segments[2] || null }, query };
}

const route = reactive(parseHash());
window.addEventListener('hashchange', () => Object.assign(route, parseHash()));

const app = createApp({
    setup() {
        const ui = reactive({ sidebarCollapsed: localStorage.getItem('sb') === '1', theme: localStorage.getItem('theme') || 'light' });
        document.documentElement.setAttribute('data-bs-theme', ui.theme);
        const currentRoute = computed(() => routes.find(r => route.path === r.path) || routes[0]);
        const currentPage = computed(() => currentRoute.value.component);
        const currentTitle = computed(() => currentRoute.value.label);
        const go = (p) => { location.hash = p; };
        const toggleTheme = () => { ui.theme = ui.theme === 'dark' ? 'light' : 'dark'; localStorage.setItem('theme', ui.theme); document.documentElement.setAttribute('data-bs-theme', ui.theme); };
        Vue.watch(() => ui.sidebarCollapsed, v => localStorage.setItem('sb', v ? '1' : '0'));
        return { ui, nav: routes, route, currentPage, currentTitle, store, help, go, toggleTheme };
    },
});
Object.entries({ ...components, 'help-hint': HelpHint, 'tour-overlay': TourOverlay }).forEach(([name, c]) => app.component(name, c));
app.config.errorHandler = (err) => { console.error(err); store.error('UI error: ' + (err && err.message ? err.message : err)); };
app.mount('#app');

store.refreshAll().then(() => help.maybeAutoStart(store));
store.startPolling(5000);
store.connectWs();
window.__store = store; // handy in the browser console

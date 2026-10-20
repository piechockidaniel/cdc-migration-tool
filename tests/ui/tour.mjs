/* Walk the guided tour end-to-end: every step must navigate to its page and find its anchor element. */
import { launch, sleep } from './browser.mjs';
process.env.TOUR = '1';
const h = await launch();
await h.page.evaluateOnNewDocument(() => localStorage.removeItem('help.tourDone'));
await h.goto('connections');
const empty = await h.page.evaluate(() => window.__store.connections.length === 0);
let active = !!(await h.page.$('.tour-pop'));
let failed = 0;
if (empty) { console.log(`${active ? '✅' : '❌'} tour auto-started on an empty instance`); if (!active) failed++; }
else { console.log(`${active ? '❌' : '✅'} tour stays quiet on a populated instance (${active ? 'auto-started!' : 'as expected'})`); if (active) failed++; }
if (!active) { await h.page.click('[data-tour="help-menu"] button'); await h.clickText('.help-menu .dropdown-item', 'Take the guided tour'); await sleep(400); }
const n = await h.page.$$eval('.tour-dots span', s => s.length);
for (let i = 0; i < n; i++) {
    await sleep(500);
    const st = await h.page.evaluate(() => ({ hash: location.hash, title: document.querySelector('.tour-pop-head strong')?.textContent, spot: getComputedStyle(document.querySelector('.tour-spot')).display !== 'none', count: document.querySelector('.tour-count')?.textContent }));
    const ok = st.title && st.spot;
    if (!ok) failed++;
    console.log(`${ok ? '✅' : '❌'} step ${st.count} ${st.hash} "${st.title}" spotlight=${st.spot}`);
    if (process.env.SHOT_DIR) await h.shot(`tour-${i + 1}`);
    if (i < n - 1) await h.clickText('.tour-pop button', 'Next'); else await h.clickText('.tour-pop button', 'Finish');
}
await sleep(300);
const gone = !(await h.page.$('.tour-pop'));
const done = await h.page.evaluate(() => localStorage.getItem('help.tourDone'));
console.log(`${gone && done === '1' ? '✅' : '❌'} tour closed and remembered`);
if (!(gone && done === '1')) failed++;
// hints: dismiss one, reset from the help menu
await h.goto('mappings');
await h.page.click('.help-hint .btn-close');
const hidden = !(await h.page.$('.help-hint'));
await h.page.click('[data-tour="help-menu"] button');
await h.clickText('.help-menu .dropdown-item', 'Show page hints again');
await sleep(200);
const back = !!(await h.page.$('.help-hint'));
console.log(`${hidden && back ? '✅' : '❌'} hint dismiss / reset (hidden=${hidden}, restored=${back})`);
if (!(hidden && back)) failed++;
// "Take the guided tour" from the menu
await h.page.click('[data-tour="help-menu"] button');
await h.clickText('.help-menu .dropdown-item', 'Take the guided tour');
await sleep(400);
const again = !!(await h.page.$('.tour-pop'));
console.log(`${again ? '✅' : '❌'} tour restarts from the help menu`);
if (!again) failed++;
if (h.errors.length) { failed++; console.log('browser errors:\n' + h.errors.join('\n')); }
await h.browser.close();
process.exit(failed ? 1 : 0);

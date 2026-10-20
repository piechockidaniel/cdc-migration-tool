/* Open every page, assert it rendered and produced no browser errors. */
import { launch } from './browser.mjs';
const h = await launch();
const routes = ['dashboard', 'connections', 'mappings', 'pipelines', 'snapshots', 'alerts', 'failures', 'audit', 'metrics', 'settings'];
let failed = 0;
for (const r of routes) {
    await h.goto(r);
    const title = await h.page.$eval('.page-title', e => e.textContent.trim()).catch(() => '');
    const body = await h.text();
    const ok = title && body.length > 40;
    if (!ok) failed++;
    console.log(`${ok ? '✅' : '❌'} ${r}: "${title}" ${body.slice(0, 90)}`);
    if (process.env.SHOT_DIR) await h.shot(r);
}
if (h.errors.length) { failed++; console.log('browser errors:\n' + h.errors.join('\n')); }
await h.browser.close();
process.exit(failed ? 1 : 0);

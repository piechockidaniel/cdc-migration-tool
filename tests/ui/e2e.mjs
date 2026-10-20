/* End-to-end scenario through the UI with the built-in demo engine:
   connection -> mapping (auto-map) -> pipeline -> snapshot -> failures/DLQ -> dashboard.
   Prerequisites: a running server (CDCSYNC_URL) and an SQLite destination file at DEST_PATH
   with table customers(id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT, city TEXT, balance REAL). */
import { launch, sleep, BASE } from './browser.mjs';
const DEST_PATH = process.env.DEST_PATH || '/tmp/cdcsync-ui-dest.sqlite3';
const h = await launch();
const { page, errors, shot, clickText, type, selectByText } = h;
// 1. demo source connection via the form
await page.goto(`${BASE}/#/connections`, { waitUntil: 'networkidle0' });
await clickText('button', 'New connection');
await page.waitForSelector('.modal-box');
await selectByText('.modal-box select', 'Demo');
await type('.modal-box input[placeholder="slug of the name if empty"]', 'demo_src');
await type('.modal-box .col-md-4:nth-of-type(3) input', 'Demo source');
// engine options: poison ratio
const numInputs = await page.$$('.modal-box input[type="number"]');
await numInputs[numInputs.length - 1].type('0.1');   // poison ratio
await clickText('.modal-box .mf button', 'Test');
await page.waitForSelector('.modal-box .alert');
console.log('test result:', await page.$eval('.modal-box .alert', e => e.textContent.trim().slice(0, 80)));
await shot('1-connection-form');
await clickText('.modal-box .mf button', 'Save');
await sleep(800);
// 2. sqlite destination via API (form covered above)
await fetch(`${BASE}/api/admin/connection/save`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ name: 'sqlite dst', server: 'x', database: 'd', engine: 'sqlite', use_windows_auth: false, options: { path: DEST_PATH } }) });
// 3. mapping via the editor with auto-map
await page.goto(`${BASE}/#/mappings`, { waitUntil: 'networkidle0' });
await page.reload({ waitUntil: 'networkidle0' }); await sleep(500);
await clickText('button', 'New mapping');
await page.waitForSelector('.modal-box');
await selectByText('.modal-box .col-md-6:nth-of-type(1) select', 'demo_src');
await sleep(800);
await selectByText('.modal-box .col-md-6:nth-of-type(1) select:nth-of-type(2)', 'demo.Customers');
await sleep(800);
await selectByText('.modal-box .col-md-6:nth-of-type(2) select', 'sqlite_dst');
await sleep(800);
await selectByText('.modal-box .col-md-6:nth-of-type(2) select:nth-of-type(2)', 'main.customers');
await sleep(800);
await clickText('.modal-box button', 'Auto-map');
await sleep(300);
// fix names: source CustomerID -> id etc. auto-map matched nothing except... adjust grid manually
await page.evaluate(() => {
  const rows = [...document.querySelectorAll('.column-grid tbody tr')];
  const map = { CustomerID: 'id', Name: 'name', Email: 'email', City: 'city', Balance: 'balance' };
  for (const r of rows) { const [src, dst] = r.querySelectorAll('input'); if (map[src.value]) { dst.value = map[src.value]; dst.dispatchEvent(new Event('input', { bubbles: true })); } }
});
await sleep(200);
await page.evaluate(() => { const el = [...document.querySelectorAll('.modal-box input')].find(i => i.closest('.col-md-4') && i.classList.contains('mono')); el.value = 'id'; el.dispatchEvent(new Event('change', { bubbles: true })); });
await shot('2-mapping-editor');
await clickText('.modal-box .mf button', 'Save');
await sleep(800);
console.log('mapping rows:', await page.$eval('main', e => e.innerText.includes('demo_customers') ? 'saved demo_customers' : e.innerText.slice(0, 200)));
// 4. pipeline
await page.goto(`${BASE}/#/pipelines`, { waitUntil: 'networkidle0' });
await clickText('button', 'Define pipeline');
await page.waitForSelector('.modal-box');
await type('.modal-box input.mono', 'demo_pipe');
await selectByText('.modal-box .col-md-4:nth-of-type(2) select', 'demo_src');
await selectByText('.modal-box .col-md-4:nth-of-type(3) select', 'sqlite_dst');
await page.click('.modal-box input[type="checkbox"][id^="pm-"]');
await clickText('.modal-box .mf button', 'Save');
await sleep(800);
await page.click(".pipeline-card button[title=\"Start\"]");
await sleep(6000);
await page.reload({ waitUntil: 'networkidle0' }); await sleep(1500);
await shot('3-pipeline-running');
console.log('pipeline:', (await page.$eval('.pipeline-card', e => e.innerText)).replace(/\s+/g, ' ').slice(0, 300));
// 5. snapshot
await page.goto(`${BASE}/#/snapshots`, { waitUntil: 'networkidle0' });
await clickText('button', 'New snapshot job');
await page.waitForSelector('.modal-box');
await selectByText('.modal-box .col-md-6:nth-of-type(1) select', 'demo_customers');
await selectByText('.modal-box .col-md-6:nth-of-type(2) select', 'demo_pipe');
await page.evaluate(() => { const i = [...document.querySelectorAll('.modal-box input[type=number]')][0]; i.value = 500; i.dispatchEvent(new Event('input', { bubbles: true })); });
await clickText('.modal-box .mf button', 'Create');
await sleep(5000);
await shot('4-snapshot');
console.log('snapshot:', (await page.$eval('main', e => e.innerText)).replace(/\s+/g, ' ').slice(0, 400));
// 6. failures
await sleep(4000);
await page.goto(`${BASE}/#/failures`, { waitUntil: 'networkidle0' }); await sleep(1500);
await shot('5-failures');
console.log('failures:', (await page.$eval('main', e => e.innerText)).replace(/\s+/g, ' ').slice(0, 500));
await page.goto(`${BASE}/#/dashboard`, { waitUntil: 'networkidle0' }); await sleep(2500);
await shot('6-dashboard');
await page.goto(`${BASE}/#/alerts`, { waitUntil: 'networkidle0' }); await sleep(1000);
await shot('7-alerts');
await page.goto(`${BASE}/#/metrics`, { waitUntil: 'networkidle0' }); await sleep(1500);
await shot('8-metrics');
console.log(errors.length ? 'ERRORS:\n' + errors.join('\n') : 'no browser errors');
await h.browser.close();
process.exit(errors.length ? 1 : 0);
